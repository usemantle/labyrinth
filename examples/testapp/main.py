"""Minimal FastAPI app for testing Labyrinth ingestors."""

import aiohttp
from fastapi import FastAPI
from fastapi.responses import Response

app = FastAPI(title="labyrinth-testapp")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/hello")
def hello():
    return {"message": "hello"}


@app.get("/files/{file_path:path}")
async def get_file(file_path: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://storage-service:8080/files/{file_path}") as resp:
            content = await resp.read()
            return Response(content=content, media_type=resp.content_type)


@app.get("/users/{user_id}")
async def get_user(user_id: int):
    """Fetch a user by ID — no auth check (IDOR vulnerability)."""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://user-service:8080/users/{user_id}") as resp:
            return await resp.json()
