import asyncio
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ["AIROPS_API_KEYS"] = "test-key-a,test-key-b,test-key-c"
os.environ["PROXY_TOKEN"] = ""
os.environ["CONFIG_PATH"] = str(Path(tempfile.gettempdir()) / "airops-proxy-test-config.json")

import httpx
from fastapi import HTTPException

import app


def model(name, app_uuid, index, input_field="input"):
    return {
        "name": name,
        "app_uuid": app_uuid,
        "input_field": input_field,
        "output_field": "",
        "input_mode": "last_user",
        "enabled": True,
        "key_index": index,
        "key_id": app.KEY_POOL.key_ids[index],
    }


class FakeRequest:
    headers = {}

    def __init__(self, body):
        self.body = body

    async def json(self):
        return self.body


class SchemaResponse:
    status_code = 200
    text = ""

    def json(self):
        return {
            "name": "Schema App",
            "inputs_schema": [{"name": "questions", "required": True}],
            "definition": [{"type": "llm", "config": {"model": "claude-opus-4-6"}}],
        }


class SchemaClient:
    post_calls = 0

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, *args, **kwargs):
        return SchemaResponse()

    async def post(self, *args, **kwargs):
        type(self).post_calls += 1
        raise AssertionError("autoconfig must not execute a workflow")


class KeyPoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_cooling_keys_return_503(self):
        pool = app.KeyPool(["a", "b"])
        until = time.monotonic() + 30
        pool.state["a"] = {"status": "cooling", "until": until}
        pool.state["b"] = {"status": "dead", "until": until}
        with self.assertRaises(HTTPException) as caught:
            await pool.get()
        self.assertEqual(caught.exception.status_code, 503)
        self.assertIn("Retry-After", caught.exception.headers)

    async def test_autoconfig_uses_schema_without_execution(self):
        SchemaClient.post_calls = 0
        with patch.object(app.httpx, "AsyncClient", SchemaClient):
            result = await app.api_autoconfig(
                FakeRequest({"app_uuid": "schema-app", "key_index": 0})
            )
        self.assertEqual(result["input_field"], "questions")
        self.assertEqual(result["llm_model"], "claude-opus-4-6")
        self.assertEqual(SchemaClient.post_calls, 0)


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.original_path = app.CONFIG_PATH
        self.original_pool = app.KEY_POOL
        self.temp_dir = tempfile.TemporaryDirectory()
        app.CONFIG_PATH = Path(self.temp_dir.name) / "config.json"
        app.KEY_POOL = app.KeyPool(["a", "b", "c"])
        app._WORKFLOW_RR_INDEX = 0

    def tearDown(self):
        app.CONFIG_PATH = self.original_path
        app.KEY_POOL = self.original_pool
        self.temp_dir.cleanup()

    def test_public_rotation_skips_dead_binding(self):
        config = {"models": [model("one", "a", 0), model("two", "b", 1), model("three", "c", 2)]}
        app._save_config(config)
        app.KEY_POOL.state["a"] = {"status": "dead", "until": time.monotonic() + 60}
        self.assertEqual(app._find_model(app.PUBLIC_MODEL_NAME)["name"], "two")
        self.assertEqual(app._find_model(app.PUBLIC_MODEL_NAME)["name"], "three")

    def test_atomic_config_round_trip_and_validation(self):
        config = {"models": [model("one", "a", 0)]}
        normalized = app._validate_config(config)
        app._save_config(normalized)
        self.assertEqual(app._load_config(), normalized)
        self.assertEqual(list(app.CONFIG_PATH.parent.glob("*.tmp")), [])
        broken = {"models": [{**config["models"][0], "input_field": ""}]}
        with self.assertRaises(HTTPException):
            app._validate_config(broken)


class InputTests(unittest.IsolatedAsyncioTestCase):
    async def test_multimodal_text_parts_are_forwarded_to_real_schema_field(self):
        captured = {}
        selected = {
            "name": "peaceful",
            "app_uuid": "workflow",
            "input_field": "questions",
            "output_field": "",
            "input_mode": "last_user",
            "enabled": True,
            "key_index": 0,
            "key_id": app.KEY_POOL.key_ids[0],
        }

        async def fake_execute(app_uuid, inputs, **kwargs):
            captured.update(inputs)
            return {"uuid": "execution"}

        async def fake_poll(*args, **kwargs):
            return {"uuid": "execution", "status": "success", "output": "received"}

        transport = httpx.ASGITransport(app=app.app)
        with patch.object(app, "_find_model", return_value=selected), patch.object(
            app, "_do_async_execute", fake_execute
        ), patch.object(app, "_poll_until_terminal", fake_poll):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.post(
                    "/v1/chat/completions",
                    json={
                        "model": app.PUBLIC_MODEL_NAME,
                        "messages": [{
                            "role": "user",
                            "content": [
                                {"type": "text", "text": "真实输入"},
                                {"type": "input_text", "text": "第二段"},
                            ],
                        }],
                    },
                )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured, {"questions": "真实输入\n第二段"})
        self.assertNotIn("usage", response.json())

    def test_empty_user_input_is_not_forwarded(self):
        self.assertEqual(app._messages_to_input([{"role": "user", "content": []}], "last_user"), "")

    def test_concat_forwards_complete_conversation_with_roles(self):
        messages = [
            {"role": "system", "content": "遵循系统要求"},
            {"role": "user", "content": "第一问"},
            {"role": "assistant", "content": "第一答"},
            {"role": "user", "content": "追问"},
        ]
        self.assertEqual(
            app._messages_to_input(messages, "concat"),
            "[system]\n遵循系统要求\n\n[user]\n第一问\n\n"
            "[assistant]\n第一答\n\n[user]\n追问",
        )

    def test_sillytavern_preserves_order_names_prefill_and_media_markers(self):
        messages = [
            {"role": "system", "content": "世界书：雨夜"},
            {"role": "assistant", "name": "Alice", "content": "第一句"},
            {"role": "system", "content": "深度注入"},
            {"role": "user", "name": "玩家", "content": [
                {"type": "text", "text": "继续"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}},
            ]},
            {"role": "assistant", "name": "Alice", "content": "预填："},
        ]
        self.assertEqual(
            app._messages_to_input(messages, "sillytavern"),
            "[system]\n世界书：雨夜\n\n"
            "[assistant name=\"Alice\"]\n第一句\n\n"
            "[system]\n深度注入\n\n"
            "[user name=\"玩家\"]\n继续\n[inline image omitted by AirOps text workflow]\n\n"
            "[assistant name=\"Alice\"]\n预填：",
        )

    async def test_sillytavern_request_parameters_and_stop_are_applied(self):
        captured = {}
        selected = {
            "name": "tavern",
            "app_uuid": "workflow",
            "input_field": "prompt",
            "output_field": "",
            "input_mode": "sillytavern",
            "request_mappings": {
                "temperature": "temperature",
                "max_tokens": "max_output_tokens",
                "stop": "stop_sequences",
            },
            "enabled": True,
            "key_index": 0,
            "key_id": app.KEY_POOL.key_ids[0],
        }

        async def fake_execute(app_uuid, inputs, **kwargs):
            captured.update(inputs)
            return {"uuid": "execution"}

        async def fake_poll(*args, **kwargs):
            return {"uuid": "execution", "status": "success", "output": "回答正文<STOP>不应返回"}

        transport = httpx.ASGITransport(app=app.app)
        with patch.object(app, "_find_model", return_value=selected), patch.object(
            app, "_do_async_execute", fake_execute
        ), patch.object(app, "_poll_until_terminal", fake_poll):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.post(
                    "/v1/chat/completions",
                    json={
                        "model": "tavern",
                        "messages": [{"role": "user", "name": "玩家", "content": "你好"}],
                        "temperature": 0.7,
                        "max_completion_tokens": 321,
                        "stop": ["<STOP>", "玩家:"],
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["prompt"], "[user name=\"玩家\"]\n你好")
        self.assertEqual(captured["temperature"], 0.7)
        self.assertEqual(captured["max_output_tokens"], 321)
        self.assertEqual(captured["stop_sequences"], ["<STOP>", "玩家:"])
        self.assertEqual(response.json()["choices"][0]["message"]["content"], "回答正文")

    async def test_multi_field_chat_completions_routes_to_workflow_fields(self):
        captured = {}
        selected = {
            "name": "multi",
            "app_uuid": "workflow",
            "input_field": "",
            "input_mappings": [
                {"role": "system", "field": "system_prompt", "mode": "last"},
                {"role": "user", "field": "question", "mode": "concat"},
                {"role": "*", "field": "context", "mode": "join"},
            ],
            "output_field": "",
            "input_mode": "concat",
            "enabled": True,
            "key_index": 0,
            "key_id": app.KEY_POOL.key_ids[0],
        }

        async def fake_execute(app_uuid, inputs, **kwargs):
            captured.update(inputs)
            return {"uuid": "execution"}

        async def fake_poll(*args, **kwargs):
            return {"uuid": "execution", "status": "success", "output": "ok"}

        transport = httpx.ASGITransport(app=app.app)
        with patch.object(app, "_find_model", return_value=selected), patch.object(
            app, "_do_async_execute", fake_execute
        ), patch.object(app, "_poll_until_terminal", fake_poll):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.post(
                    "/v1/chat/completions",
                    json={
                        "model": "multi",
                        "messages": [
                            {"role": "system", "content": "你是助手"},
                            {"role": "user", "content": "第一问"},
                            {"role": "assistant", "content": "第一答"},
                            {"role": "user", "content": "追问"},
                        ],
                    },
                )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(captured["system_prompt"], "你是助手")
        self.assertEqual(captured["question"], "第一问\n\n追问")
        self.assertEqual(captured["context"], "[assistant]\n第一答")


class MultiSchemaResponse:
    status_code = 200
    text = ""

    def json(self):
        return {
            "name": "Multi App",
            "inputs_schema": [
                {"name": "system_prompt", "required": True},
                {"name": "question", "required": True},
            ],
            "definition": [{"type": "llm", "config": {"model": "gpt-4o"}}],
        }


class MultiSchemaClient:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, *args, **kwargs):
        return MultiSchemaResponse()

    async def post(self, *args, **kwargs):
        raise AssertionError("autoconfig must not execute a workflow")


class TavernSchemaResponse:
    status_code = 200
    text = ""

    def json(self):
        return {
            "name": "Tavern App",
            "inputs_schema": [
                {"name": "prompt", "required": True},
                {"name": "temperature", "required": True},
                {"name": "max_output_tokens", "required": False},
                {"name": "stop_sequences", "required": False},
            ],
            "definition": [{"type": "llm", "config": {"model": "gpt-4o"}}],
        }


class TavernSchemaClient(MultiSchemaClient):
    async def get(self, *args, **kwargs):
        return TavernSchemaResponse()


class InputMappingsTests(unittest.TestCase):
    def test_multi_field_routes_by_role(self):
        messages = [
            {"role": "system", "content": "系统要求"},
            {"role": "user", "content": "第一问"},
            {"role": "assistant", "content": "第一答"},
            {"role": "user", "content": "追问"},
        ]
        model = {
            "input_field": "",
            "input_mode": "concat",
            "input_mappings": [
                {"role": "system", "field": "sys", "mode": "last"},
                {"role": "user", "field": "q", "mode": "concat"},
                {"role": "*", "field": "ctx", "mode": "join"},
            ],
        }
        result = app._messages_to_inputs(messages, model)
        self.assertEqual(result["sys"], "系统要求")
        self.assertEqual(result["q"], "第一问\n\n追问")
        self.assertEqual(result["ctx"], "[assistant]\n第一答")

    def test_multi_field_falls_back_to_single_when_no_mappings(self):
        messages = [{"role": "user", "content": "hi"}]
        model = {"input_field": "topic", "input_mode": "concat"}
        result = app._messages_to_inputs(messages, model)
        self.assertEqual(result, {"topic": "[user]\nhi"})

    def test_multi_field_wildcard_captures_unmapped_roles(self):
        messages = [
            {"role": "user", "content": "问题"},
            {"role": "assistant", "content": "回答"},
            {"role": "user", "content": "追问"},
        ]
        model = {
            "input_field": "",
            "input_mappings": [
                {"role": "user", "field": "q", "mode": "last"},
                {"role": "*", "field": "ctx", "mode": "join"},
            ],
        }
        result = app._messages_to_inputs(messages, model)
        self.assertEqual(result["q"], "追问")
        self.assertEqual(result["ctx"], "[assistant]\n回答")

    def test_multi_field_all_fields_present_even_if_empty(self):
        messages = [{"role": "user", "content": "hi"}]
        model = {
            "input_field": "",
            "input_mappings": [
                {"role": "system", "field": "sys", "mode": "last"},
                {"role": "user", "field": "q", "mode": "last"},
            ],
        }
        result = app._messages_to_inputs(messages, model)
        self.assertEqual(result, {"sys": "", "q": "hi"})

    def test_test_inputs_for_targets_user_in_multi_mode(self):
        model = {
            "input_field": "",
            "input_mappings": [
                {"role": "system", "field": "sys", "mode": "last"},
                {"role": "user", "field": "q", "mode": "concat"},
            ],
        }
        result = app._test_inputs_for(model, "ping")
        self.assertEqual(result, {"sys": "", "q": "ping"})

    def test_test_inputs_for_single_mode_uses_input_field(self):
        model = {"input_field": "topic", "input_mode": "concat"}
        result = app._test_inputs_for(model, "ping")
        self.assertEqual(result, {"topic": "ping"})

    def test_validate_config_accepts_input_mappings(self):
        cfg = {"models": [{
            "name": "m", "app_uuid": "uuid", "input_field": "",
            "input_mappings": [{"role": "user", "field": "q", "mode": "last"}],
            "key_index": 0, "key_id": app.KEY_POOL.key_ids[0],
        }]}
        normalized = app._validate_config(cfg)
        self.assertEqual(normalized["models"][0]["input_mappings"][0]["role"], "user")

    def test_validate_config_rejects_duplicate_wildcard(self):
        cfg = {"models": [{
            "name": "m", "app_uuid": "uuid", "input_field": "",
            "input_mappings": [
                {"role": "*", "field": "a", "mode": "last"},
                {"role": "*", "field": "b", "mode": "last"},
            ],
            "key_index": 0, "key_id": app.KEY_POOL.key_ids[0],
        }]}
        with self.assertRaises(HTTPException) as caught:
            app._validate_config(cfg)
        self.assertEqual(caught.exception.status_code, 400)

    def test_validate_config_rejects_invalid_mapping_mode(self):
        cfg = {"models": [{
            "name": "m", "app_uuid": "uuid", "input_field": "",
            "input_mappings": [{"role": "user", "field": "q", "mode": "bogus"}],
            "key_index": 0, "key_id": app.KEY_POOL.key_ids[0],
        }]}
        with self.assertRaises(HTTPException):
            app._validate_config(cfg)

    def test_validate_config_rejects_duplicate_workflow_input_fields(self):
        cfg = {"models": [{
            "name": "m", "app_uuid": "uuid", "input_field": "",
            "input_mappings": [
                {"role": "system", "field": "prompt", "mode": "last"},
                {"role": "user", "field": "prompt", "mode": "concat"},
            ],
            "key_index": 0, "key_id": app.KEY_POOL.key_ids[0],
        }]}
        with self.assertRaises(HTTPException):
            app._validate_config(cfg)

    def test_validate_config_accepts_sillytavern_and_request_mappings(self):
        cfg = {"models": [{
            "name": "m", "app_uuid": "uuid", "input_field": "prompt",
            "input_mode": "sillytavern",
            "request_mappings": {"temperature": "temperature", "stop": "stop_sequences"},
            "key_index": 0, "key_id": app.KEY_POOL.key_ids[0],
        }]}
        normalized = app._validate_config(cfg)["models"][0]
        self.assertEqual(normalized["input_mode"], "sillytavern")
        self.assertEqual(normalized["request_mappings"]["stop"], "stop_sequences")

    def test_validate_config_rejects_request_mapping_collision(self):
        cfg = {"models": [{
            "name": "m", "app_uuid": "uuid", "input_field": "prompt",
            "input_mode": "sillytavern",
            "request_mappings": {"temperature": "prompt"},
            "key_index": 0, "key_id": app.KEY_POOL.key_ids[0],
        }]}
        with self.assertRaises(HTTPException):
            app._validate_config(cfg)


class AutoConfigMultiTests(unittest.IsolatedAsyncioTestCase):
    async def test_autoconfig_multi_required_returns_mappings(self):
        with patch.object(app.httpx, "AsyncClient", MultiSchemaClient):
            result = await app.api_autoconfig(FakeRequest({"app_uuid": "multi-app", "key_index": 0}))
        self.assertIn("input_mappings", result)
        self.assertTrue(result["input_mappings"])
        roles = {m["role"] for m in result["input_mappings"]}
        self.assertIn("user", roles)
        self.assertIn("system", roles)
        self.assertTrue(result.get("multi_field"))
        self.assertEqual(result["llm_model"], "gpt-4o")

    async def test_autoconfig_single_required_still_returns_input_field(self):
        with patch.object(app.httpx, "AsyncClient", SchemaClient):
            result = await app.api_autoconfig(FakeRequest({"app_uuid": "schema-app", "key_index": 0}))
        self.assertNotIn("input_mappings", result)
        self.assertEqual(result["input_field"], "questions")

    async def test_autoconfig_separates_chat_parameters_from_conversation_fields(self):
        with patch.object(app.httpx, "AsyncClient", TavernSchemaClient):
            result = await app.api_autoconfig(FakeRequest({"app_uuid": "tavern-app", "key_index": 0}))
        self.assertNotIn("input_mappings", result)
        self.assertEqual(result["input_field"], "prompt")
        self.assertEqual(result["input_mode"], "sillytavern")
        self.assertEqual(result["request_mappings"], {
            "temperature": "temperature",
            "max_tokens": "max_output_tokens",
            "stop": "stop_sequences",
        })


class StaticSafetyTests(unittest.TestCase):
    def test_frontend_avoids_unsafe_html_and_persistent_token(self):
        html = (Path(__file__).parents[1] / "static" / "index.html").read_text(encoding="utf-8")
        self.assertNotIn("innerHTML", html)
        self.assertNotIn("localStorage", html)

    def test_launcher_uses_pwsh_and_dependencies_are_pinned(self):
        root = Path(__file__).parents[1]
        launcher = (root / "run.bat").read_text(encoding="utf-8").lower()
        start_script = (root / "start.ps1").read_text(encoding="utf-8").lower()
        compose = (root / "compose.yaml").read_text(encoding="utf-8")
        dockerignore = (root / ".dockerignore").read_text(encoding="utf-8").splitlines()
        requirements = (root / "requirements.txt").read_text(encoding="utf-8")
        self.assertIn("pwsh", launcher)
        self.assertNotIn("powershell ", launcher)
        self.assertIn("docker", start_script)
        self.assertIn("127.0.0.1", compose)
        self.assertIn(".env", dockerignore)
        self.assertNotIn(">=", requirements)


if __name__ == "__main__":
    unittest.main()
