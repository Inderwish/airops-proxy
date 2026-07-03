"""
AirOps Workflow → OpenAI 兼容 API 代理

三件事：
1. 透明反代 AirOps 官方端点（自动注入 Bearer Key）
2. OpenAI 兼容层：/v1/models, /v1/chat/completions（含 SSE 流式）
3. 管理面板 GUI（static/index.html）— 配置 model↔workflow 映射、查看执行历史

配置优先级：环境变量 > config.json > 默认值
配置文件 config.json 由管理面板维护，结构:
{
  "models": [
    {
      "name": "my-wf",            // OpenAI 客户端用的 model 名
      "app_uuid": "uuid-...",      // AirOps app uuid
      "input_field": "topic",      // 单字段模式：messages 拼接后填入此字段
      "output_field": "output",    // 从 execution.output 取此字段；空则取整个 output
      "input_mode": "sillytavern", // sillytavern=保序并保留角色名/预填; concat=兼容拼接; last_user=仅最后 user
      "input_mappings": [           // 多字段模式（可选；非空时覆盖 input_field/input_mode）
        {"role": "system", "field": "system_prompt", "mode": "last"},
        {"role": "user", "field": "question", "mode": "concat"},
        {"role": "*", "field": "context", "mode": "join"}  // 兜底捕获未映射 role
      ],
      "request_mappings": {         // 可选：把酒馆/OpenAI 生成参数送入 workflow 字段
        "temperature": "temperature",
        "max_tokens": "max_tokens",
        "stop": "stop_sequences"
      },
      "enabled": true,
      "key_index": 0,               // 兼容展示；key 换序后由 key_id 修正
      "key_id": "stable-hash"       // key 的稳定摘要，不含 key 明文
    }
  ]
}

环境变量见 .env.example。
"""

import os
import time
import json
import uuid as uuidlib
import asyncio
import hashlib
import threading
import logging
from collections import OrderedDict
from pathlib import Path

import httpx
from fastapi import FastAPI, Request, Response, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles

# ========== 配置 ==========
AIROPS_BASE = os.environ.get("AIROPS_BASE_URL", "https://api.airops.com").rstrip("/")
PROXY_TOKEN = os.environ.get("PROXY_TOKEN", "")
DOCKER_BIND_HOST = os.environ.get("DOCKER_BIND_HOST", "").strip().lower()
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "2"))
POLL_TIMEOUT = float(os.environ.get("POLL_TIMEOUT", "600"))
CONFIG_PATH = Path(os.environ.get("CONFIG_PATH", "config.json"))
if not CONFIG_PATH.is_absolute():
    CONFIG_PATH = Path(__file__).parent / CONFIG_PATH
STATIC_DIR = Path(__file__).parent / "static"
TERMINAL = {"success", "error", "cancelled", "review_needed"}
KEY_COOLDOWN = float(os.environ.get("AIROPS_KEY_COOLDOWN", "60"))
PUBLIC_MODEL_NAME = os.environ.get(
    "AIROPS_PUBLIC_MODEL", "fable5"
).strip()
if POLL_INTERVAL <= 0 or POLL_TIMEOUT <= 0:
    raise RuntimeError("POLL_INTERVAL 和 POLL_TIMEOUT 必须大于 0")
if not PUBLIC_MODEL_NAME:
    raise RuntimeError("AIROPS_PUBLIC_MODEL 不能为空")
if DOCKER_BIND_HOST not in ("", "127.0.0.1", "localhost", "::1") and not PROXY_TOKEN:
    raise RuntimeError("Docker 发布到非本机地址时必须配置 PROXY_TOKEN")
_WORKFLOW_RR_INDEX = 0
_WORKFLOW_RR_LOCK = threading.Lock()
_CONFIG_LOCK = threading.RLock()
_EXECUTION_BINDINGS: OrderedDict[str, str] = OrderedDict()
_EXECUTION_BINDINGS_LOCK = threading.Lock()
_MAX_EXECUTION_BINDINGS = 10_000
logger = logging.getLogger("airops-proxy")

# SillyTavern 的 Custom Chat Completion 会发送这些 OpenAI/扩展采样参数。
# AirOps workflow 只接收 inputs，因此通过 request_mappings 显式绑定到 schema 字段。
_CHAT_OPTION_KEYS = {
    "temperature",
    "max_tokens",
    "top_p",
    "top_k",
    "min_p",
    "top_a",
    "presence_penalty",
    "frequency_penalty",
    "repetition_penalty",
    "stop",
    "seed",
    "n",
    "logit_bias",
}
_CHAT_OPTION_FIELD_ALIASES = {
    "temperature": ("temperature", "temp"),
    "max_tokens": ("max_tokens", "max_output_tokens", "response_tokens"),
    "top_p": ("top_p",),
    "top_k": ("top_k",),
    "min_p": ("min_p",),
    "top_a": ("top_a",),
    "presence_penalty": ("presence_penalty",),
    "frequency_penalty": ("frequency_penalty",),
    "repetition_penalty": ("repetition_penalty", "repeat_penalty"),
    "stop": ("stop", "stop_sequences", "stopping_strings"),
    "seed": ("seed",),
    "n": ("n", "candidate_count"),
    "logit_bias": ("logit_bias",),
}

# 解析 key 列表：AIROPS_API_KEYS（逗号分隔）优先，回退到单个 AIROPS_API_KEY
_raw_keys = os.environ.get("AIROPS_API_KEYS", "").strip()
if not _raw_keys:
    _single = os.environ.get("AIROPS_API_KEY", "").strip()
    _raw_keys = _single
_keys = [k.strip() for k in _raw_keys.split(",") if k.strip()]
if not _keys:
    raise RuntimeError("未配置 AirOps key：设置 AIROPS_API_KEY 或 AIROPS_API_KEYS（多 key 逗号分隔）")


class KeyPool:
    """多 key 轮换池。round-robin 取用，401 标记失效，429 标记冷却。dead 也会自动恢复。"""

    def __init__(self, keys: list):
        self.keys = keys
        self.key_ids = [self._make_id(key) for key in keys]
        self.idx = 0
        self.state = {k: {"status": "ok", "until": 0.0} for k in keys}
        self.lock = threading.RLock()

    @staticmethod
    def _make_id(key: str) -> str:
        """生成不暴露 key 内容、且不受列表顺序影响的稳定标识。"""
        return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]

    def resolve(self, key_index: int | None, key_id: str = "") -> tuple[str, int, str]:
        """解析持久化的 key 绑定。优先使用稳定 key_id，兼容旧的 key_index。"""
        if key_id:
            try:
                resolved_index = self.key_ids.index(key_id)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"配置绑定的 key_id={key_id} 不在当前 AIROPS_API_KEYS 中",
                )
            return self.keys[resolved_index], resolved_index, key_id
        if isinstance(key_index, bool) or not isinstance(key_index, int):
            raise HTTPException(status_code=400, detail="key_index 必须是整数")
        if key_index < 0 or key_index >= len(self.keys):
            raise HTTPException(
                status_code=400,
                detail=f"key_index={key_index} 越界（0-{len(self.keys)-1}）",
            )
        return self.keys[key_index], key_index, self.key_ids[key_index]

    async def get(self) -> str:
        """取一个可用 key，跳过冷却/失效的（到期自动恢复）。全部不可用则抛 503。"""
        with self.lock:
            now = time.monotonic()
            self._refresh_locked(now)
            # round-robin 找可用 key
            for _ in range(len(self.keys)):
                k = self.keys[self.idx]
                self.idx = (self.idx + 1) % len(self.keys)
                if self.state[k]["status"] == "ok":
                    return k
            waits = [
                max(1, int(state["until"] - now + 0.999))
                for state in self.state.values()
                if state["until"] > now
            ]
            retry_after = min(waits) if waits else 1
            raise HTTPException(
                status_code=503,
                detail={"error": "所有 key 暂不可用", "retry_after": retry_after},
                headers={"Retry-After": str(retry_after)},
            )

    def _refresh_locked(self, now: float) -> None:
        for state in self.state.values():
            if state["status"] in ("cooling", "dead") and now >= state["until"]:
                state["status"] = "ok"
                state["until"] = 0.0

    def status_for(self, key: str) -> str:
        with self.lock:
            self._refresh_locked(time.monotonic())
            return self.state[key]["status"]

    def ensure_available(self, key: str) -> None:
        with self.lock:
            now = time.monotonic()
            self._refresh_locked(now)
            state = self.state[key]
            if state["status"] == "ok":
                return
            retry_after = max(1, int(state["until"] - now + 0.999))
            raise HTTPException(
                status_code=503,
                detail={
                    "error": f"workflow 绑定的 key 处于 {state['status']} 状态",
                    "retry_after": retry_after,
                },
                headers={"Retry-After": str(retry_after)},
            )

    async def fail(self, key: str, status_code: int) -> None:
        """按上游状态码标记 key。401=失效（5 分钟后自动恢复），429=冷却。"""
        with self.lock:
            if status_code == 401:
                # dead 也加恢复时间，避免临时 401 导致永久不可用
                self.state[key] = {"status": "dead", "until": time.monotonic() + 300}
            elif status_code == 429:
                self.state[key] = {"status": "cooling", "until": time.monotonic() + KEY_COOLDOWN}

    def reset(self) -> None:
        """重置所有 key 状态为 ok。"""
        with self.lock:
            for k in self.state:
                self.state[k] = {"status": "ok", "until": 0.0}

    def snapshot(self) -> list:
        """返回各 key 状态快照供面板显示。"""
        with self.lock:
            self._refresh_locked(time.monotonic())
            return [
                {"key": key[:8] + "…", "status": self.state[key]["status"]}
                for key in self.keys
            ]


KEY_POOL = KeyPool(_keys)

app = FastAPI(title="AirOps → OpenAI Proxy", version="2.1")


# ========== 工具 ==========
def _client_auth(request: Request) -> None:
    """可选的代理鉴权。兼容 X-Proxy-Token 和 OpenAI 标准 Authorization: Bearer。"""
    if not PROXY_TOKEN:
        return
    token = request.headers.get("x-proxy-token", "")
    if not token:
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            token = auth[7:]
    if token != PROXY_TOKEN:
        raise HTTPException(status_code=401, detail="invalid token")


def _upstream_headers(key: str, extra: dict | None = None) -> dict:
    """构造使用明确 key 的上游请求头，禁止隐式选择 workspace。"""
    h = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    if extra:
        h.update(extra)
    return h


async def _upstream_headers_async(extra: dict | None = None) -> tuple[dict, str]:
    """异步取 key 并返回 (headers, key)。key 用于失败时回标。"""
    key = await KEY_POOL.get()
    h = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    if extra:
        h.update(extra)
    return h, key


def _remember_execution(exec_uuid: str, key_id: str) -> None:
    if not exec_uuid or not key_id:
        return
    with _EXECUTION_BINDINGS_LOCK:
        _EXECUTION_BINDINGS[exec_uuid] = key_id
        _EXECUTION_BINDINGS.move_to_end(exec_uuid)
        while len(_EXECUTION_BINDINGS) > _MAX_EXECUTION_BINDINGS:
            _EXECUTION_BINDINGS.popitem(last=False)


def _execution_key_id(exec_uuid: str) -> str:
    with _EXECUTION_BINDINGS_LOCK:
        return _EXECUTION_BINDINGS.get(exec_uuid, "")


def _configured_app_binding(app_uuid: str) -> tuple[str, int, str] | None:
    for model in _load_config().get("models", []):
        if model.get("app_uuid") == app_uuid:
            return KEY_POOL.resolve(model.get("key_index"), model.get("key_id", ""))
    return None


async def _resolve_app_binding(app_uuid: str) -> tuple[str, int, str]:
    configured = _configured_app_binding(app_uuid)
    if configured:
        KEY_POOL.ensure_available(configured[0])
        return configured
    errors = []
    async with httpx.AsyncClient(timeout=30) as client:
        for index, key in enumerate(KEY_POOL.keys):
            if KEY_POOL.status_for(key) != "ok":
                continue
            try:
                response = await client.get(
                    f"{AIROPS_BASE}/public_api/airops_apps/{app_uuid}",
                    headers=_upstream_headers(key),
                )
            except httpx.HTTPError as error:
                errors.append(f"key{index}: {error}")
                continue
            if response.status_code == 200:
                return key, index, KEY_POOL.key_ids[index]
            if response.status_code in (401, 429):
                await KEY_POOL.fail(key, response.status_code)
            elif response.status_code != 404:
                errors.append(f"key{index}: HTTP {response.status_code}")
    if errors:
        raise HTTPException(status_code=502, detail={"error": "定位 workflow key 失败", "causes": errors})
    raise HTTPException(status_code=404, detail=f"workflow {app_uuid} 不属于任何已配置 key")


async def _resolve_execution_binding(exec_uuid: str) -> tuple[str, int, str]:
    known_key_id = _execution_key_id(exec_uuid)
    if known_key_id:
        binding = KEY_POOL.resolve(None, known_key_id)
        KEY_POOL.ensure_available(binding[0])
        return binding
    errors = []
    async with httpx.AsyncClient(timeout=30) as client:
        for index, key in enumerate(KEY_POOL.keys):
            if KEY_POOL.status_for(key) != "ok":
                continue
            try:
                response = await client.get(
                    f"{AIROPS_BASE}/public_api/airops_apps/executions/{exec_uuid}",
                    headers=_upstream_headers(key),
                )
            except httpx.HTTPError as error:
                errors.append(f"key{index}: {error}")
                continue
            if response.status_code == 200:
                key_id = KEY_POOL.key_ids[index]
                _remember_execution(exec_uuid, key_id)
                return key, index, key_id
            if response.status_code in (401, 429):
                await KEY_POOL.fail(key, response.status_code)
            elif response.status_code != 404:
                errors.append(f"key{index}: HTTP {response.status_code}")
    if errors:
        raise HTTPException(status_code=502, detail={"error": "定位 execution key 失败", "causes": errors})
    raise HTTPException(status_code=404, detail=f"execution {exec_uuid} 不属于任何已配置 key")


def _proxy_response(response: httpx.Response) -> Response:
    headers = {}
    for name in ("content-type", "retry-after", "x-request-id"):
        if value := response.headers.get(name):
            headers[name] = value
    return Response(content=response.content, status_code=response.status_code, headers=headers)


def _remember_from_response(response: httpx.Response, key_id: str) -> dict:
    try:
        payload = response.json()
    except ValueError:
        return {}
    if not isinstance(payload, dict):
        return {}
    execution = payload.get("airops_app_execution") or payload
    if isinstance(execution, dict):
        _remember_execution(execution.get("uuid", ""), key_id)
    return payload


def _request_headers_for_key(request: Request, key: str) -> dict:
    headers = {"Authorization": f"Bearer {key}"}
    for name in ("content-type", "accept"):
        if value := request.headers.get(name):
            headers[name] = value
    return headers


async def _list_apps_across_keys(params=None) -> tuple[list[tuple[dict, int]], list[dict]]:
    async def fetch(index: int, key: str):
        status = KEY_POOL.status_for(key)
        if status != "ok":
            return [], {"key_index": index, "error": f"key 状态为 {status}"}
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    f"{AIROPS_BASE}/public_api/airops_apps",
                    headers=_upstream_headers(key),
                    params=params,
                )
        except httpx.HTTPError as error:
            return [], {"key_index": index, "error": str(error)}
        if response.status_code in (401, 429):
            await KEY_POOL.fail(key, response.status_code)
        if response.status_code != 200:
            return [], {"key_index": index, "error": f"HTTP {response.status_code}"}
        try:
            apps = response.json()
        except ValueError:
            return [], {"key_index": index, "error": "无效 JSON"}
        if not isinstance(apps, list):
            return [], {"key_index": index, "error": "响应不是 app 数组"}
        return [(item, index) for item in apps if isinstance(item, dict)], None

    results = await asyncio.gather(
        *(fetch(index, key) for index, key in enumerate(KEY_POOL.keys))
    )
    merged = []
    errors = []
    for apps, error in results:
        merged.extend(apps)
        if error:
            errors.append(error)
    return merged, errors


async def _do_async_execute(
    app_uuid: str,
    inputs: dict,
    key_index: int | None = None,
    key_id: str = "",
) -> dict:
    """提交异步执行。key_id/key_index 指定 workspace key；未绑定则轮换。
    401/429 自动切 key 重试。"""
    bound_key = KEY_POOL.resolve(key_index, key_id)[0] if key_index is not None or key_id else None
    if bound_key is not None:
        KEY_POOL.ensure_available(bound_key)
    for attempt in range(len(KEY_POOL.keys)):
        if bound_key is not None:
            key = bound_key
            headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        else:
            headers, key = await _upstream_headers_async()
        try:
            async with httpx.AsyncClient(timeout=60) as c:
                r = await c.post(
                    f"{AIROPS_BASE}/public_api/airops_apps/{app_uuid}/async_execute",
                    headers=headers, json={"inputs": inputs},
                )
        except httpx.HTTPError as error:
            raise HTTPException(status_code=502, detail=f"AirOps 连接失败: {error}")
        if r.status_code in (401, 429):
            await KEY_POOL.fail(key, r.status_code)
            if bound_key is not None:
                # 绑定 key 失败，直接报错（切到别的 key 也调不到这个 workspace 的 app）
                raise HTTPException(status_code=502, detail=f"workflow 绑定的 key 不可用: {r.text[:200]}")
            continue
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail=f"AirOps async_execute: {r.text[:300]}")
        try:
            execution = r.json().get("airops_app_execution", {})
        except ValueError:
            raise HTTPException(status_code=502, detail="AirOps async_execute 返回了无效 JSON")
        exec_uuid = execution.get("uuid")
        if not exec_uuid:
            raise HTTPException(status_code=502, detail="AirOps async_execute 未返回 execution uuid")
        resolved_key_id = KEY_POOL._make_id(key)
        _remember_execution(exec_uuid, resolved_key_id)
        return execution
    raise HTTPException(status_code=503, detail="所有 key 均不可用")


async def _poll_until_terminal(
    exec_uuid: str,
    timeout: float,
    key_index: int | None = None,
    key_id: str = "",
) -> dict:
    """轮询 execution uuid 直到终态，并保持使用提交时绑定的 key。"""
    deadline = time.monotonic() + timeout
    bound_key = key_index is not None or bool(key_id)
    if bound_key:
        key = KEY_POOL.resolve(key_index, key_id)[0]
        KEY_POOL.ensure_available(key)
    else:
        key = await KEY_POOL.get()
    async with httpx.AsyncClient(timeout=60) as c:
        while True:
            headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
            try:
                r = await c.get(
                    f"{AIROPS_BASE}/public_api/airops_apps/executions/{exec_uuid}",
                    headers=headers,
                )
            except httpx.HTTPError as error:
                raise HTTPException(status_code=502, detail=f"AirOps poll 连接失败: {error}")
            if r.status_code in (401, 429):
                await KEY_POOL.fail(key, r.status_code)
                if bound_key:
                    raise HTTPException(status_code=502, detail=f"poll 时 key 不可用: {r.text[:200]}")
                key = await KEY_POOL.get()
                continue
            if r.status_code != 200:
                raise HTTPException(status_code=502, detail=f"AirOps poll: {r.text[:300]}")
            try:
                execution = r.json()
            except ValueError:
                raise HTTPException(status_code=502, detail="AirOps poll 返回了无效 JSON")
            if execution.get("status") in TERMINAL:
                return execution
            if time.monotonic() >= deadline:
                raise HTTPException(
                    status_code=504,
                    detail={"error": "polling timeout", "execution": execution},
                )
            await asyncio.sleep(POLL_INTERVAL)


def _load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {"models": []}
    try:
        with _CONFIG_LOCK:
            cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        logger.exception("读取配置失败: %s", CONFIG_PATH)
        raise HTTPException(status_code=500, detail=f"配置文件读取失败: {error}")
    if not isinstance(cfg, dict) or not isinstance(cfg.get("models"), list):
        raise HTTPException(status_code=500, detail="配置文件必须是 {models: [...]} 结构")
    return cfg


def _save_config(cfg: dict) -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = CONFIG_PATH.with_name(f".{CONFIG_PATH.name}.{uuidlib.uuid4().hex}.tmp")
    payload = json.dumps(cfg, ensure_ascii=False, indent=2) + "\n"
    try:
        with _CONFIG_LOCK:
            with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, CONFIG_PATH)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _validate_config(cfg: object) -> dict:
    if not isinstance(cfg, dict) or not isinstance(cfg.get("models"), list):
        raise HTTPException(status_code=400, detail="需要 {models: [...]} 结构")
    normalized = dict(cfg)
    normalized_models = []
    names = set()
    for index, raw_model in enumerate(cfg["models"]):
        if not isinstance(raw_model, dict):
            raise HTTPException(status_code=400, detail=f"models[{index}] 必须是对象")
        model = dict(raw_model)
        # input_mappings 存在时走多字段模式，input_field/input_mode 可省略
        raw_mappings = model.get("input_mappings")
        has_mappings = isinstance(raw_mappings, list) and len(raw_mappings) > 0
        required_fields = ["name", "app_uuid"]
        if not has_mappings:
            required_fields.append("input_field")
        for field in required_fields:
            value = model.get(field)
            if not isinstance(value, str) or not value.strip():
                raise HTTPException(status_code=400, detail=f"models[{index}].{field} 不能为空")
            model[field] = value.strip()
        # input_field 可能为空字符串（多字段模式），归一化
        model.setdefault("input_field", "")
        if not isinstance(model["input_field"], str):
            raise HTTPException(status_code=400, detail=f"models[{index}].input_field 必须是字符串")
        model["input_field"] = model["input_field"].strip()
        if model["name"] in names:
            raise HTTPException(status_code=400, detail=f"model 名重复: {model['name']}")
        names.add(model["name"])
        if not isinstance(model.get("enabled", True), bool):
            raise HTTPException(status_code=400, detail=f"models[{index}].enabled 必须是布尔值")
        model["enabled"] = model.get("enabled", True)
        input_mode = model.get("input_mode", "concat")
        if input_mode not in ("last_user", "concat", "sillytavern"):
            raise HTTPException(status_code=400, detail=f"models[{index}].input_mode 无效")
        model["input_mode"] = input_mode
        output_field = model.get("output_field", "")
        if not isinstance(output_field, str):
            raise HTTPException(status_code=400, detail=f"models[{index}].output_field 必须是字符串")
        model["output_field"] = output_field.strip()
        # 校验 input_mappings
        if "input_mappings" in model:
            if not isinstance(raw_mappings, list):
                raise HTTPException(status_code=400, detail=f"models[{index}].input_mappings 必须是数组")
            normalized_mappings = []
            mapping_fields = set()
            wildcard_count = 0
            for mi, raw_map in enumerate(raw_mappings):
                if not isinstance(raw_map, dict):
                    raise HTTPException(status_code=400, detail=f"models[{index}].input_mappings[{mi}] 必须是对象")
                role = str(raw_map.get("role", "")).strip().lower() or "*"
                field = str(raw_map.get("field", "")).strip()
                if not field:
                    raise HTTPException(status_code=400, detail=f"models[{index}].input_mappings[{mi}].field 不能为空")
                mode = str(raw_map.get("mode", "concat")).strip().lower()
                if mode not in ("last", "concat", "join"):
                    raise HTTPException(status_code=400, detail=f"models[{index}].input_mappings[{mi}].mode 无效: {mode}")
                if role == "*":
                    wildcard_count += 1
                if field in mapping_fields:
                    raise HTTPException(
                        status_code=400,
                        detail=f"models[{index}].input_mappings 字段重复: {field}",
                    )
                normalized_mappings.append({"role": role, "field": field, "mode": mode})
                mapping_fields.add(field)
            if wildcard_count > 1:
                raise HTTPException(status_code=400, detail=f"models[{index}].input_mappings 只能有一个 role='*' 兜底")
            model["input_mappings"] = normalized_mappings
        # request_mappings 把 Chat Completion 参数显式送入 workflow inputs。
        raw_request_mappings = model.get("request_mappings", {})
        if not isinstance(raw_request_mappings, dict):
            raise HTTPException(status_code=400, detail=f"models[{index}].request_mappings 必须是对象")
        normalized_request_mappings = {}
        request_fields = set()
        conversation_fields = mapping_fields if has_mappings else {model["input_field"]}
        for raw_key, raw_field in raw_request_mappings.items():
            key_name = str(raw_key).strip().lower()
            if key_name not in _CHAT_OPTION_KEYS:
                raise HTTPException(
                    status_code=400,
                    detail=f"models[{index}].request_mappings 不支持参数: {key_name}",
                )
            if not isinstance(raw_field, str) or not raw_field.strip():
                raise HTTPException(
                    status_code=400,
                    detail=f"models[{index}].request_mappings.{key_name} 字段不能为空",
                )
            target_field = raw_field.strip()
            if target_field in conversation_fields:
                raise HTTPException(
                    status_code=400,
                    detail=f"models[{index}].request_mappings 字段与消息输入冲突: {target_field}",
                )
            if target_field in request_fields:
                raise HTTPException(
                    status_code=400,
                    detail=f"models[{index}].request_mappings 字段重复: {target_field}",
                )
            normalized_request_mappings[key_name] = target_field
            request_fields.add(target_field)
        if normalized_request_mappings:
            model["request_mappings"] = normalized_request_mappings
        else:
            model.pop("request_mappings", None)
        key, resolved_index, key_id = KEY_POOL.resolve(
            model.get("key_index"), model.get("key_id", "")
        )
        del key
        model["key_index"] = resolved_index
        model["key_id"] = key_id
        normalized_models.append(model)
    normalized["models"] = normalized_models
    return normalized


def _find_model(name: str) -> dict | None:
    """把公开 LLM 名或兼容 workflow 别名解析为具体 workflow。"""
    cfg = _load_config()
    models = [m for m in cfg.get("models", []) if m.get("enabled", True)]

    # workflow 名继续作为兼容别名；明确名称优先于公开包装名。
    for m in models:
        if m.get("name") == name:
            return m

    # 公开模型名按配置顺序轮询所有启用的 workflow。
    if name == PUBLIC_MODEL_NAME:
        available_models = []
        for model in models:
            try:
                if model.get("key_id") or model.get("key_index") is not None:
                    key = KEY_POOL.resolve(
                        model.get("key_index"), model.get("key_id", "")
                    )[0]
                    if KEY_POOL.status_for(key) != "ok":
                        continue
                available_models.append(model)
            except HTTPException as error:
                logger.warning("跳过无效 workflow 配置 %s: %s", model.get("name"), error.detail)
        if not available_models:
            raise HTTPException(status_code=503, detail="没有可用的 workflow")
        global _WORKFLOW_RR_INDEX
        with _WORKFLOW_RR_LOCK:
            model = available_models[_WORKFLOW_RR_INDEX % len(available_models)]
            _WORKFLOW_RR_INDEX = (_WORKFLOW_RR_INDEX + 1) % len(available_models)
        return model

    # 兼容旧配置中保存过的 llm_model 名。
    for m in models:
        llm = m.get("llm_model", "")
        if llm:
            if name == _normalize_model_name(llm) or name == llm:
                return m
    return None


def _normalize_model_name(llm: str) -> str:
    """把 AirOps 内部 LLM 名规范化为 airops/<vendor>-<ver>-<variant>。
    claude-opus-4-6 → airops/claude-4-6-opus
    gpt-4o-mini    → airops/gpt-4o-mini（非 claude 原样加前缀）
    """
    llm = llm.strip()
    if not llm:
        return "airops/unknown"
    if llm.startswith("airops/"):
        return llm
    # Anthropic: claude-<variant>-<version> → claude-<version>-<variant>
    import re
    m = re.match(r"^(claude)-([a-z]+)-(\d[\w.-]*)$", llm, re.I)
    if m:
        vendor, variant, ver = m.group(1).lower(), m.group(2).lower(), m.group(3)
        return f"airops/{vendor}-{ver}-{variant}"
    # 其他直接加前缀
    return f"airops/{llm}"


def _extract_llm_from_definition(definition: list) -> str:
    """从 workflow definition 提取第一个 LLM 步的 model 名。"""
    for step in definition or []:
        if step.get("type") == "llm":
            model = step.get("config", {}).get("model", "")
            if model:
                return model
    return ""


_ROLE_HINTS = {
    "system": ("system", "prompt", "instruction", "preamble", "persona", "role"),
    "user": ("user", "question", "query", "topic", "input", "message", "ask", "text", "content", "data"),
    "assistant": ("assistant", "response", "answer", "output", "reply", "result"),
}


def _guess_role_for_field(field_name: str) -> str:
    """按字段名启发式猜 role：system/user/assistant，命中不了返回 ""。"""
    name = field_name.lower()
    for role, hints in _ROLE_HINTS.items():
        if any(hint in name for hint in hints):
            return role
    return ""


def _guess_mappings_for_fields(fields: list[str]) -> tuple[list[dict], set[str]]:
    """为多个必填字段生成 input_mappings 建议。

    策略：
    1. 按 name 启发式分配 system/user/assistant
    2. 没命中启发式的，按出现顺序轮流分配 user/assistant（优先 user）
    3. 如果没有任何字段映射到 user，把第一个字段强制设为 user
    返回 (mappings, used_fields)。
    """
    mappings: list[dict] = []
    used: set[str] = set()
    assigned_roles: set[str] = set()
    remaining: list[str] = []
    for field in fields:
        role = _guess_role_for_field(field)
        if role and role not in assigned_roles:
            mappings.append({"role": role, "field": field, "mode": "last"})
            used.add(field)
            assigned_roles.add(role)
        else:
            remaining.append(field)
    # 剩余字段轮流分配 user/assistant（user 优先）
    role_cycle = ["user", "assistant", "system"]
    for field in remaining:
        role = next((r for r in role_cycle if r not in assigned_roles), "user")
        mappings.append({"role": role, "field": field, "mode": "last"})
        used.add(field)
        assigned_roles.add(role)
    # 确保 user 至少有一个
    if not any(m["role"] == "user" for m in mappings):
        if mappings:
            mappings[0]["role"] = "user"
    return mappings, used


def _guess_request_mappings(schema_fields: list[dict]) -> dict[str, str]:
    """按精确字段别名识别 workflow 的采样参数输入，避免把它们误判成对话 role。"""
    available = {
        field["name"].strip().lower(): field["name"].strip()
        for field in schema_fields
        if isinstance(field, dict) and isinstance(field.get("name"), str) and field["name"].strip()
    }
    result = {}
    used_fields = set()
    for option, aliases in _CHAT_OPTION_FIELD_ALIASES.items():
        for alias in aliases:
            target = available.get(alias)
            if target and target not in used_fields:
                result[option] = target
                used_fields.add(target)
                break
    return result


def _extract_output(execution: dict, output_field: str) -> str:
    """从 execution.output 提取文本。output_field 为空则取整个 output 的 JSON。"""
    output = execution.get("output")
    if output is None:
        return ""
    if output_field:
        # 支持 a.b.c 路径
        cur = output
        for part in output_field.split("."):
            if isinstance(cur, dict):
                cur = cur.get(part)
            elif isinstance(cur, list) and part.isdigit() and int(part) < len(cur):
                cur = cur[int(part)]
            else:
                cur = None
                break
        if cur is None:
            return json.dumps(output, ensure_ascii=False)
        return str(cur) if not isinstance(cur, str) else cur
    if isinstance(output, str):
        return output
    # 尝试找第一个字符串字段
    if isinstance(output, dict):
        for v in output.values():
            if isinstance(v, str):
                return v
    return json.dumps(output, ensure_ascii=False)


def _content_to_text(content: object) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict):
                text = part.get("text") or part.get("input_text")
                if isinstance(text, str):
                    parts.append(text)
        return "\n".join(parts)
    if isinstance(content, dict):
        text = content.get("text") or content.get("input_text")
        return text if isinstance(text, str) else ""
    return ""


def _sillytavern_content_to_text(content: object) -> str:
    """保留文本顺序；无法作为字符串传给 AirOps 的媒体用短占位符明确标记。"""
    if not isinstance(content, list):
        return _content_to_text(content)
    parts = []
    for part in content:
        if isinstance(part, str):
            parts.append(part)
            continue
        if not isinstance(part, dict):
            continue
        text = part.get("text") or part.get("input_text")
        if isinstance(text, str):
            parts.append(text)
            continue
        part_type = str(part.get("type", "")).strip().lower()
        if part_type in ("image", "image_url", "input_image"):
            parts.append("[inline image omitted by AirOps text workflow]")
        elif part_type in ("audio", "input_audio"):
            parts.append("[inline audio omitted by AirOps text workflow]")
        elif part_type in ("video", "video_url", "input_video"):
            parts.append("[inline video omitted by AirOps text workflow]")
        elif part_type in ("file", "input_file"):
            parts.append("[inline file omitted by AirOps text workflow]")
    return "\n".join(parts)


def _safe_message_label(value: object, default: str) -> str:
    text = str(value if value is not None else default).strip().lower() or default
    return text if all(char.isalnum() or char in "_-" for char in text) else default


def _messages_to_sillytavern_input(messages: list) -> str:
    """把酒馆已经编排好的消息按原顺序串行化，不按 role 重排。

    末尾 assistant 消息保持在原位，因此 Continue Prefill 不会被吞掉；群聊角色名、
    tool_call_id 和工具调用也会保留为可读文本。媒体无法送入字符串 workflow，使用占位符。
    """
    parts = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        role = _safe_message_label(message.get("role"), "user")
        attributes = []
        name = message.get("name")
        if isinstance(name, str) and name.strip():
            attributes.append(f"name={json.dumps(name.strip(), ensure_ascii=False)}")
        tool_call_id = message.get("tool_call_id")
        if isinstance(tool_call_id, str) and tool_call_id.strip():
            attributes.append(f"tool_call_id={json.dumps(tool_call_id.strip(), ensure_ascii=False)}")
        header = f"[{role}{(' ' + ' '.join(attributes)) if attributes else ''}]"
        body_parts = []
        content = _sillytavern_content_to_text(message.get("content", ""))
        if content:
            body_parts.append(content)
        refusal = message.get("refusal")
        if isinstance(refusal, str) and refusal:
            body_parts.append(f"[refusal]\n{refusal}")
        tool_calls = message.get("tool_calls")
        if isinstance(tool_calls, list) and tool_calls:
            body_parts.append(
                "[tool_calls]\n" + json.dumps(tool_calls, ensure_ascii=False, separators=(",", ":"))
            )
        parts.append(header + "\n" + "\n\n".join(body_parts))
    return "\n\n".join(parts)


def _messages_to_input(messages: list, mode: str) -> str:
    """把 OpenAI messages 拼接成 workflow 单一输入字符串。"""
    if not isinstance(messages, list):
        raise HTTPException(status_code=400, detail="messages 必须是数组")
    if mode == "last_user":
        for m in reversed(messages):
            if isinstance(m, dict) and m.get("role") == "user":
                return _content_to_text(m.get("content", ""))
        return ""
    if mode == "sillytavern":
        return _messages_to_sillytavern_input(messages)
    # concat: 全部消息按 role: content 拼接
    parts = []
    for m in messages:
        if not isinstance(m, dict):
            continue
        role = m.get("role", "user")
        content = _content_to_text(m.get("content", ""))
        parts.append(f"[{role}]\n{content}")
    return "\n\n".join(parts)


def _request_options_to_inputs(request_body: dict, model: dict) -> dict:
    """根据 model.request_mappings 提取酒馆/OpenAI 生成参数。"""
    mappings = model.get("request_mappings") if isinstance(model, dict) else None
    if not isinstance(mappings, dict):
        return {}
    result = {}
    for option, field in mappings.items():
        if option not in _CHAT_OPTION_KEYS or not isinstance(field, str) or not field:
            continue
        if option == "max_tokens":
            value = request_body.get("max_completion_tokens")
            if value is None:
                value = request_body.get("max_tokens")
        else:
            value = request_body.get(option)
        if value is not None:
            result[field] = value
    return result


def _stop_sequences(value: object) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str) and item]
    return []


def _apply_stop_sequences(content: str, stop: object) -> str:
    """AirOps 无法中止已开始的 workflow；返回前仍按最早 stop 截断以兼容客户端语义。"""
    positions = [content.find(sequence) for sequence in _stop_sequences(stop)]
    positions = [position for position in positions if position >= 0]
    return content[:min(positions)] if positions else content


def _messages_to_inputs(messages: list, model: dict) -> dict:
    """构造 workflow inputs 字典。

    两种模式：
    - 多字段模式（model.input_mappings 非空）：按 role 分别路由到各自 field。
      mappings 每项 {role, field, mode}。role="*" 为兜底，捕获未单独映射的 role。
      mode: last=取该 role 最后一条；concat=该 role 所有消息用 \\n\\n 拼接；
            join=所有命中消息带 [role] 前缀拼接（适合 * 兜底保留上下文）。
    - 单字段模式（兼容）：用 model.input_field + model.input_mode。
    """
    if not isinstance(messages, list):
        raise HTTPException(status_code=400, detail="messages 必须是数组")
    mappings = model.get("input_mappings") if isinstance(model, dict) else None
    if mappings and isinstance(mappings, list):
        return _messages_to_inputs_multi(messages, mappings)
    text = _messages_to_input(messages, model.get("input_mode", "concat") if isinstance(model, dict) else "concat")
    field = (model.get("input_field", "") if isinstance(model, dict) else "") or "input"
    return {field: text}


def _messages_to_inputs_multi(messages: list, mappings: list) -> dict:
    """按 input_mappings 把 messages 路由到多个 workflow 字段。"""
    # 归一化 mappings，校验
    normalized = []
    fields_seen = set()
    for index, raw in enumerate(mappings):
        if not isinstance(raw, dict):
            raise HTTPException(status_code=400, detail=f"input_mappings[{index}] 必须是对象")
        role = str(raw.get("role", "")).strip().lower() or "*"
        field = str(raw.get("field", "")).strip()
        if not field:
            raise HTTPException(status_code=400, detail=f"input_mappings[{index}].field 不能为空")
        mode = str(raw.get("mode", "concat")).strip().lower()
        if mode not in ("last", "concat", "join"):
            raise HTTPException(status_code=400, detail=f"input_mappings[{index}].mode 无效: {mode}")
        normalized.append({"role": role, "field": field, "mode": mode})
        fields_seen.add(field)

    # 按 role 分桶收集消息文本
    by_role: dict[str, list[str]] = {}
    order: list[str] = []
    for m in messages:
        if not isinstance(m, dict):
            continue
        role = str(m.get("role", "user")).strip().lower() or "user"
        text = _content_to_text(m.get("content", ""))
        if role not in by_role:
            by_role[role] = []
            order.append(role)
        by_role[role].append(text)

    result: dict[str, str] = {}
    wildcard = None
    for entry in normalized:
        if entry["role"] == "*":
            wildcard = entry
            continue
        texts = by_role.get(entry["role"], [])
        if not texts:
            continue
        if entry["mode"] == "last":
            result[entry["field"]] = texts[-1]
        elif entry["mode"] == "concat":
            result[entry["field"]] = "\n\n".join(texts)
        else:  # join
            result[entry["field"]] = "\n\n".join(f"[{entry['role']}]\n{t}" for t in texts)

    if wildcard is not None:
        # 收集所有未被单独映射的 role
        mapped_roles = {e["role"] for e in normalized if e["role"] != "*"}
        leftover = [(role, by_role[role]) for role in order if role not in mapped_roles]
        if leftover:
            if wildcard["mode"] == "last":
                _, texts = leftover[-1]
                result[wildcard["field"]] = texts[-1]
            elif wildcard["mode"] == "concat":
                result[wildcard["field"]] = "\n\n".join(t for _, texts in leftover for t in texts)
            else:  # join
                result[wildcard["field"]] = "\n\n".join(
                    f"[{role}]\n{t}" for role, texts in leftover for t in texts
                )

    # 确保所有映射字段都出现在结果里（空字符串也算，避免 workflow 报缺字段）
    for entry in normalized:
        result.setdefault(entry["field"], "")
    return result


def _test_inputs_for(model: dict, test_input: str) -> dict:
    """为 /api/test 构造测试 inputs：把单个测试字符串填到合适字段。

    多字段模式：优先填 user 映射字段，其次第一个映射字段，其余字段留空字符串。
    单字段模式：填到 input_field。
    """
    mappings = model.get("input_mappings") if isinstance(model, dict) else None
    if mappings and isinstance(mappings, list) and mappings:
        result = {entry["field"]: "" for entry in mappings if isinstance(entry, dict) and entry.get("field")}
        target_field = ""
        for entry in mappings:
            if isinstance(entry, dict) and str(entry.get("role", "")).lower() == "user":
                target_field = entry.get("field", "")
                break
        if not target_field:
            for entry in mappings:
                if isinstance(entry, dict) and entry.get("field"):
                    target_field = entry["field"]
                    break
        if target_field:
            result[target_field] = test_input
        return result
    field = (model.get("input_field", "") if isinstance(model, dict) else "") or "input"
    return {field: test_input}





# ========== 透明反代（保留原端点）==========
@app.api_route("/apps", methods=["GET"])
async def list_apps(request: Request):
    _client_auth(request)
    apps, errors = await _list_apps_across_keys(request.query_params.multi_items())
    if errors:
        raise HTTPException(
            status_code=502,
            detail={"error": "部分 workspace 拉取失败", "causes": errors, "partial_count": len(apps)},
        )
    return [item for item, _ in apps]


@app.api_route("/apps/{app_uuid}", methods=["GET"])
async def get_app(app_uuid: str, request: Request):
    _client_auth(request)
    key, _, _ = await _resolve_app_binding(app_uuid)
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(
                f"{AIROPS_BASE}/public_api/airops_apps/{app_uuid}",
                headers=_request_headers_for_key(request, key),
                params=request.query_params.multi_items(),
            )
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail=f"AirOps 连接失败: {error}")
    return _proxy_response(response)


@app.api_route("/apps/{app_uuid}/execute", methods=["POST"])
async def execute(app_uuid: str, request: Request):
    _client_auth(request)
    body = await request.body()
    key, _, key_id = await _resolve_app_binding(app_uuid)
    try:
        async with httpx.AsyncClient(timeout=700) as client:
            response = await client.post(
                f"{AIROPS_BASE}/public_api/airops_apps/{app_uuid}/execute",
                headers=_request_headers_for_key(request, key),
                params=request.query_params.multi_items(),
                content=body,
            )
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail=f"AirOps 连接失败: {error}")
    _remember_from_response(response, key_id)
    return _proxy_response(response)


@app.api_route("/apps/{app_uuid}/async_execute", methods=["POST"])
async def async_execute(app_uuid: str, request: Request):
    _client_auth(request)
    body = await request.body()
    key, _, key_id = await _resolve_app_binding(app_uuid)
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                f"{AIROPS_BASE}/public_api/airops_apps/{app_uuid}/async_execute",
                headers=_request_headers_for_key(request, key),
                params=request.query_params.multi_items(),
                content=body,
            )
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail=f"AirOps 连接失败: {error}")
    _remember_from_response(response, key_id)
    return _proxy_response(response)


@app.api_route("/apps/{app_uuid}/webhook_async_execute", methods=["POST"])
async def webhook_async_execute(app_uuid: str, request: Request):
    _client_auth(request)
    body = await request.body()
    key, _, key_id = await _resolve_app_binding(app_uuid)
    params = [(name, value) for name, value in request.query_params.multi_items() if name != "auth_token"]
    params.append(("auth_token", key))
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                f"{AIROPS_BASE}/public_api/airops_apps/{app_uuid}/webhook_async_execute",
                headers={"Content-Type": request.headers.get("content-type", "application/json")},
                params=params,
                content=body,
            )
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail=f"AirOps 连接失败: {error}")
    _remember_from_response(response, key_id)
    return _proxy_response(response)


@app.api_route("/executions/{execution_uuid}", methods=["GET"])
async def get_execution(execution_uuid: str, request: Request):
    _client_auth(request)
    key, _, _ = await _resolve_execution_binding(execution_uuid)
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(
                f"{AIROPS_BASE}/public_api/airops_apps/executions/{execution_uuid}",
                headers=_request_headers_for_key(request, key),
                params=request.query_params.multi_items(),
            )
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail=f"AirOps 连接失败: {error}")
    return _proxy_response(response)


@app.api_route("/executions/{execution_uuid}/cancel", methods=["PATCH"])
async def cancel_execution(execution_uuid: str, request: Request):
    _client_auth(request)
    key, _, _ = await _resolve_execution_binding(execution_uuid)
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.patch(
                f"{AIROPS_BASE}/public_api/airops_apps/executions/{execution_uuid}/cancel",
                headers=_request_headers_for_key(request, key),
                params=request.query_params.multi_items(),
            )
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail=f"AirOps 连接失败: {error}")
    return _proxy_response(response)


@app.api_route("/executions/{execution_uuid}/retry", methods=["POST"])
async def retry_execution(execution_uuid: str, request: Request):
    _client_auth(request)
    key, _, key_id = await _resolve_execution_binding(execution_uuid)
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                f"{AIROPS_BASE}/public_api/airops_apps/executions/{execution_uuid}/retry",
                headers=_request_headers_for_key(request, key),
                params=request.query_params.multi_items(),
            )
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail=f"AirOps 连接失败: {error}")
    _remember_from_response(response, key_id)
    return _proxy_response(response)


@app.api_route("/executions/{execution_uuid}/feedback", methods=["PATCH"])
async def rate_execution(execution_uuid: str, request: Request):
    _client_auth(request)
    body = await request.body()
    key, _, _ = await _resolve_execution_binding(execution_uuid)
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.patch(
                f"{AIROPS_BASE}/public_api/airops_apps/executions/{execution_uuid}/feedback",
                headers=_request_headers_for_key(request, key),
                params=request.query_params.multi_items(),
                content=body,
            )
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail=f"AirOps 连接失败: {error}")
    return _proxy_response(response)


@app.api_route("/run/{app_uuid}", methods=["POST"])
async def run_with_polling(
    app_uuid: str,
    request: Request,
    wait: bool = Query(False),
    timeout: float = Query(POLL_TIMEOUT, gt=0, le=3600),
):
    """async_execute + 可选轮询。wait=true 时轮询到终态。"""
    _client_auth(request)
    body = await request.body()
    key, key_index, key_id = await _resolve_app_binding(app_uuid)
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                f"{AIROPS_BASE}/public_api/airops_apps/{app_uuid}/async_execute",
                headers=_request_headers_for_key(request, key),
                content=body,
            )
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail=f"AirOps 连接失败: {error}")
    if response.status_code != 200:
        return _proxy_response(response)
    payload = _remember_from_response(response, key_id)
    execution = payload.get("airops_app_execution", {})
    exec_uuid = execution.get("uuid")
    if not exec_uuid or not wait:
        return _proxy_response(response)
    try:
        execution = await _poll_until_terminal(
            exec_uuid, timeout, key_index=key_index, key_id=key_id
        )
    except HTTPException as e:
        return JSONResponse(e.detail, status_code=e.status_code, headers=e.headers)
    return JSONResponse(execution, status_code=200)


# ========== 管理 API ==========
@app.get("/api/config")
async def api_get_config(request: Request):
    _client_auth(request)
    return _load_config()


@app.post("/api/config")
async def api_save_config(request: Request):
    _client_auth(request)
    cfg = _validate_config(await request.json())
    _save_config(cfg)
    return {"ok": True}


@app.get("/api/keys")
async def api_keys_status(request: Request):
    """返回各 key 状态。"""
    _client_auth(request)
    return {"keys": KEY_POOL.snapshot(), "total": len(KEY_POOL.keys)}


@app.post("/api/keys/reset")
async def api_keys_reset(request: Request):
    """重置所有 key 状态为 ok。"""
    _client_auth(request)
    KEY_POOL.reset()
    return {"ok": True, "keys": KEY_POOL.snapshot()}


@app.get("/api/apps")
async def api_list_remote_apps(request: Request):
    """从 AirOps 拉取所有 key 对应 workspace 的 app 列表，合并返回。
    每个 app 标注来源 key_index，供 autoconfig 绑定。"""
    _client_auth(request)
    apps, errors = await _list_apps_across_keys()
    if errors:
        raise HTTPException(
            status_code=502,
            detail={"error": "部分 workspace 拉取失败", "causes": errors, "partial_count": len(apps)},
        )
    return [{
        "uuid": remote.get("uuid"),
        "name": remote.get("name"),
        "description": remote.get("description", ""),
        "workspace_id": remote.get("workspace_id"),
        "key_index": index,
        "key_id": KEY_POOL.key_ids[index],
        "key_preview": KEY_POOL.keys[index][:8] + "…",
    } for remote, index in apps]


@app.post("/api/autoconfig")
async def api_autoconfig(request: Request):
    """全自动探测 app 的输入/输出字段，返回完整 model 配置建议。
    body: {app_uuid: "...", key_index: 0}  — key_index 指定用哪个 workspace 的 key。
    """
    _client_auth(request)
    body = await request.json()
    app_uuid = body.get("app_uuid", "")
    key_index = body.get("key_index", 0)
    key_id = body.get("key_id", "")
    if not app_uuid:
        raise HTTPException(status_code=400, detail="需要 app_uuid")
    key, key_index, key_id = KEY_POOL.resolve(key_index, key_id)
    up_h = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

    async with httpx.AsyncClient(timeout=30) as c:
        try:
            r = await c.get(
                f"{AIROPS_BASE}/public_api/airops_apps/{app_uuid}",
                headers=up_h,
            )
        except httpx.HTTPError as error:
            raise HTTPException(status_code=502, detail=f"读取 workflow 定义失败: {error}")
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"读取 workflow 定义失败: {r.text[:300]}")
    try:
        app_info = r.json()
    except ValueError:
        raise HTTPException(status_code=502, detail="AirOps 返回了无效的 workflow JSON")

    app_name = app_info.get("name", "workflow")
    inputs_schema = app_info.get("inputs_schema") or []
    schema_fields = [
        field for field in inputs_schema
        if isinstance(field, dict) and isinstance(field.get("name"), str) and field["name"].strip()
    ]
    request_mappings = _guess_request_mappings(schema_fields)
    request_field_names = set(request_mappings.values())
    conversation_fields = [field for field in schema_fields if field["name"].strip() not in request_field_names]
    required_fields = [field for field in conversation_fields if field.get("required")]
    probe_log = []
    if request_mappings:
        probe_log.append({"step": "request_parameter_fields", "mappings": request_mappings})

    import re
    # 多必填字段：按字段名启发式分配 role，生成 input_mappings
    if len(required_fields) > 1:
        mappings, used_fields = _guess_mappings_for_fields(
            [field["name"].strip() for field in required_fields]
        )
        probe_log.append({"step": "inputs_schema_multi", "mappings": mappings})
        llm_model = _extract_llm_from_definition(app_info.get("definition", []))
        slug = re.sub(r"[^a-z0-9-]", "-", app_name.lower()).strip("-")
        slug = re.sub(r"-+", "-", slug) or "workflow"
        result = {
            "name": slug,
            "app_uuid": app_uuid,
            "app_name": app_name,
            "input_field": "",
            "input_mappings": mappings,
            "output_field": "",
            "input_mode": "sillytavern",
            "enabled": True,
            "llm_model": llm_model,
            "key_index": key_index,
            "key_id": key_id,
            "probe_log": probe_log,
            "output_sample": None,
            "multi_field": True,
            "unmapped_required": [f for f in (field["name"].strip() for field in conversation_fields if field.get("required")) if f not in used_fields],
        }
        if request_mappings:
            result["request_mappings"] = request_mappings
        return result

    selected_field = required_fields[0] if required_fields else (conversation_fields[0] if conversation_fields else None)

    if selected_field:
        input_field = selected_field["name"].strip()
        probe_log.append({"step": "inputs_schema", "field": input_field})
    else:
        definition_json = json.dumps(app_info.get("definition", []), ensure_ascii=False)
        variables = re.findall(r"{{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*}}", definition_json)
        input_field = variables[0] if variables else ""
        if input_field:
            probe_log.append({"step": "definition variable", "field": input_field})
    if not input_field:
        raise HTTPException(status_code=422, detail="workflow 定义中没有可识别的输入字段")

    output_field = ""
    output_sample = None

    # 生成 model 名（slugify）+ 提取 LLM model 名；不执行 workflow，不产生探测费用或副作用。
    slug = re.sub(r"[^a-z0-9-]", "-", app_name.lower()).strip("-")
    slug = re.sub(r"-+", "-", slug) or "workflow"
    llm_model = _extract_llm_from_definition(app_info.get("definition", []))

    result = {
        "name": slug,
        "app_uuid": app_uuid,
        "app_name": app_name,
        "input_field": input_field,
        "output_field": output_field,
        "input_mode": "sillytavern",
        "enabled": True,
        "llm_model": llm_model,
        "key_index": key_index,
        "key_id": key_id,
        "probe_log": probe_log,
        "output_sample": output_sample,
    }
    if request_mappings:
        result["request_mappings"] = request_mappings
    return result


@app.post("/api/test")
async def api_test_model(request: Request):
    """测试某 model 映射。
    body 支持两种形式：
      1) {model: "name", input: "hello"}            — 按名字查 config.json
      2) {model_config: {...}, input: "hello"}      — 内联传配置，不依赖磁盘
    model_config 字段: name, app_uuid, input_field, output_field, input_mode, enabled
    """
    _client_auth(request)
    body = await request.json()
    model = None
    if isinstance(body.get("model_config"), dict):
        model = body["model_config"]
    elif body.get("model"):
        model = _find_model(body["model"])
    if not model:
        raise HTTPException(status_code=404, detail="model 未配置或未启用（未保存？用 model_config 内联传或先点保存）")
    if not model.get("app_uuid"):
        raise HTTPException(status_code=400, detail="model 配置缺 app_uuid")
    test_input = body.get("input", "ping")
    inputs = _test_inputs_for(model, test_input)
    ki = model.get("key_index")
    key_id = model.get("key_id", "")
    try:
        execution = await _do_async_execute(model["app_uuid"], inputs, key_index=ki, key_id=key_id)
        execution = await _poll_until_terminal(
            execution["uuid"], POLL_TIMEOUT, key_index=ki, key_id=key_id
        )
    except HTTPException as e:
        return JSONResponse(e.detail, status_code=e.status_code, headers=e.headers)
    text = _extract_output(execution, model.get("output_field", ""))
    return {"status": execution["status"], "text": text, "uuid": execution.get("uuid")}


# ========== OpenAI 兼容层 ==========
@app.get("/v1/models")
async def v1_models(request: Request):
    _client_auth(request)
    models = _load_config().get("models", [])
    enabled = any(m.get("enabled", True) for m in models)
    data = []
    if enabled:
        data.append({
            "id": PUBLIC_MODEL_NAME,
            "object": "model",
            "created": 0,
            "owned_by": "airops",
        })
    return {"object": "list", "data": data}


@app.post("/v1/chat/completions")
async def v1_chat_completions(request: Request):
    """
    OpenAI Chat Completions 兼容端点。
    - model 名映射到 config.json 中的 app_uuid
    - messages 拼接后填入 workflow 的 input_field
    - async_execute + 轮询拿结果
    - stream=true 时用 SSE 伪流式吐出（先拿完整结果再分块）
    """
    _client_auth(request)
    req = await request.json()
    model_name = req.get("model", "")
    messages = req.get("messages", [])
    stream = bool(req.get("stream", False))

    model = _find_model(model_name)
    if not model:
        raise HTTPException(status_code=404, detail=f"model '{model_name}' 未配置")

    inputs = _messages_to_inputs(messages, model)
    if not any(v.strip() for v in inputs.values()):
        raise HTTPException(status_code=400, detail="没有读取到非空的用户输入")
    option_inputs = _request_options_to_inputs(req, model)
    conflicts = set(inputs).intersection(option_inputs)
    if conflicts:
        raise HTTPException(status_code=500, detail=f"workflow 输入字段配置冲突: {', '.join(sorted(conflicts))}")
    inputs.update(option_inputs)

    try:
        ki = model.get("key_index")
        key_id = model.get("key_id", "")
        execution = await _do_async_execute(
            model["app_uuid"], inputs, key_index=ki, key_id=key_id
        )
        execution = await _poll_until_terminal(
            execution["uuid"], POLL_TIMEOUT, key_index=ki, key_id=key_id
        )
    except HTTPException as e:
        return JSONResponse(e.detail, status_code=e.status_code, headers=e.headers)

    if execution.get("status") != "success":
        err = execution.get("error_message") or execution.get("error_code") or execution["status"]
        raise HTTPException(status_code=502, detail=f"workflow {execution['status']}: {err}")

    content = _extract_output(execution, model.get("output_field", ""))
    content = _apply_stop_sequences(content, req.get("stop"))
    cid = "chatcmpl-" + uuidlib.uuid4().hex
    created = int(time.time())

    if not stream:
        return {
            "id": cid,
            "object": "chat.completion",
            "created": created,
            "model": model_name,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }],
        }

    # SSE 伪流式：拿完整结果后按词切片吐出
    async def gen():
        def chunks(s, n=4):
            for i in range(0, len(s), n):
                yield s[i:i + n]
        first = {
            "id": cid, "object": "chat.completion.chunk", "created": created, "model": model_name,
            "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": None}],
        }
        yield f"data: {json.dumps(first)}\n\n"
        for piece in chunks(content):
            chunk = {
                "id": cid, "object": "chat.completion.chunk", "created": created, "model": model_name,
                "choices": [{"index": 0, "delta": {"content": piece}, "finish_reason": None}],
            }
            yield f"data: {json.dumps(chunk)}\n\n"
            await asyncio.sleep(0.02)
        last = {
            "id": cid, "object": "chat.completion.chunk", "created": created, "model": model_name,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        }
        yield f"data: {json.dumps(last)}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


# ========== 前端管理面板 ==========
@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


if __name__ == "__main__":
    import uvicorn
    host = os.environ.get("HOST", "127.0.0.1")
    if host not in ("127.0.0.1", "localhost", "::1") and not PROXY_TOKEN:
        raise RuntimeError("监听非本机地址时必须配置 PROXY_TOKEN")
    uvicorn.run("app:app", host=host, port=int(os.environ.get("PORT", "8080")))
