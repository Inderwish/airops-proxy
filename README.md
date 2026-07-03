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

管理面板维护的 `config.json` 存放在 Docker 命名卷 `airops-data` 中，重建
容器不会丢失。首次创建该卷时，会自动使用仓库里的 `config.json` 初始化。

常用命令：

```powershell
docker compose logs -f
docker compose down
```
