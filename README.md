##Free Tier is no longer available

# AirOps Proxy

## Starting with Docker

1. Copy `.env.example` to `.env` and fill in at least `AIROPS_API_KEY` or `AIROPS_API_KEYS`.
2. Double-click `run.bat`, or run in pwsh:

   ```powershell
   docker compose up -d --build
   ```

3. Open <http://127.0.0.1:8081>. The OpenAI-compatible API endpoint is `http://127.0.0.1:8081/v1`.

By default, it only listens on the host's localhost address. To change the host port, set `DOCKER_PORT` in `.env`. To allow LAN access, set `DOCKER_BIND_HOST=0.0.0.0`, and be sure to also set `PROXY_TOKEN`.

The `config.json` managed by the admin panel is stored in the Docker named volume `airops-data`, and will not be lost when the container is rebuilt. When the volume is first created, it will be automatically initialized with the `config.json` from the repository.

Common commands:

```powershell
docker compose logs -f
docker compose down
```
