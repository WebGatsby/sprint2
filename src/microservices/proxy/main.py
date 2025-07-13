import os
import random
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

app = FastAPI()

# URLs сервисов
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")

# Флаги миграции
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))

@app.get("/health")
async def health():
    return JSONResponse(content={"status": "ok"})

@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(full_path: str, request: Request):
    # Определяем, куда направлять запрос
    target_url = MONOLITH_URL
    if full_path.startswith("movies"):
        if GRADUAL_MIGRATION and random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT:
            target_url = MOVIES_SERVICE_URL
    elif full_path.startswith("events"):
        target_url = EVENTS_SERVICE_URL

    # Подготовка запроса
    method = request.method
    url = f"{target_url}/{full_path}"
    headers = dict(request.headers)
    body = await request.body()

    # Выполняем проксирование
    async with httpx.AsyncClient() as client:
        response = await client.request(method, url, headers=headers, content=body)

    # Удаляем конфликтные заголовки
    excluded_headers = {"content-length", "transfer-encoding", "connection"}
    safe_headers = {
        k: v for k, v in response.headers.items()
        if k.lower() not in excluded_headers
    }

    return Response(
        content=response.content,
        status_code=response.status_code,
        headers=safe_headers,
        media_type=response.headers.get("content-type")
    )
