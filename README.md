# AirOps Proxy

## Docker 启动

1. 从 `.env.example` 复制生成 `.env`，至少填写 `AIROPS_API_KEY` 或
   `AIROPS_API_KEYS`。
2. 双击 `run.bat`，或在 pwsh 中运行：

   ```powershell
   docker compose up -d --build
   ```

3. 打开 <http://127.0.0.1:8081>。OpenAI 兼容 API 地址为
   `http://127.0.0.1:8081/v1`。

默认只监听宿主机本地地址。需要修改宿主机端口时，在 `.env` 中设置
`DOCKER_PORT`。若要允许局域网访问，设置 `DOCKER_BIND_HOST=0.0.0.0`，并务必
同时设置 `PROXY_TOKEN`。

## SillyTavern / 酒馆

在酒馆中选择 `Chat Completion` → `Custom (OpenAI-compatible)`，Base URL 填
`http://127.0.0.1:8081/v1`。仓库自带的 workflow 配置已使用 `sillytavern`
输入模式：Prompt Manager 生成的 System / User / Assistant 消息会保持原顺序，
群聊角色 `name`、Continue Prefill 和工具调用上下文也会保留为文本。

酒馆会按所选模型 tokenizer 管理上下文窗口，因此代理不会再次裁剪消息。
AirOps 的字符串输入无法接收内联图片、音频或视频，这些内容会变成明确的占位符。

如果 workflow 的 inputs schema 含有 `temperature`、`top_p`、`max_tokens`、
`stop_sequences` 等同名参数字段，自动配置会生成 `request_mappings`。也可以手动配置：

```json
"request_mappings": {
  "temperature": "temperature",
  "top_p": "top_p",
  "max_tokens": "max_tokens",
  "stop": "stop_sequences"
}
```

即使 workflow 没有 stop 输入，代理仍会在响应返回酒馆前按最早命中的停止字符串截断。

管理面板维护的 `config.json` 存放在 Docker 命名卷 `airops-data` 中，重建
容器不会丢失。首次创建该卷时，会自动使用仓库里的 `config.json` 初始化。

常用命令：

```powershell
docker compose logs -f
docker compose down
```
