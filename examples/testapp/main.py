"""Minimal FastAPI app for testing Labyrinth ingestors."""

import aiohttp
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pathlib import PurePosixPath

app = FastAPI(title="labyrinth-testapp")


@app.get("/ping")
def ping():
    return {"message": "pong"}


@app.get("/files/{file_path:path}")
async def get_file(file_path: str):
    # Sanitize path: reject traversal sequences ("..")  and absolute paths.
    # PurePosixPath splits the path into its components so we can inspect
    # each segment individually, guarding against both plain "../.." and
    # URL-decoded variants that FastAPI has already decoded for us.
    try:
        parts = PurePosixPath(file_path).parts
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid file path")

    if not parts or any(part == ".." for part in parts) or parts[0] == "/":
        raise HTTPException(status_code=400, detail="Invalid file path")

    safe_path = "/".join(parts)
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://storage-service:8080/files/{safe_path}") as resp:
            content = await resp.read()
            return Response(content=content, media_type=resp.content_type)


@app.get("/users/{user_id}")
async def get_user(user_id: int):
    """Fetch a user by ID — no auth check (IDOR vulnerability)."""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://user-service:8080/users/{user_id}") as resp:
            return await resp.json()
