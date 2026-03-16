"""Minimal FastAPI app for testing Labyrinth ingestors."""

import os

import aiohttp
from fastapi import FastAPI, HTTPException, Security
from fastapi.responses import Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

app = FastAPI(title="labyrinth-testapp")

_bearer = HTTPBearer()


def _verify_token(credentials: HTTPAuthorizationCredentials = Security(_bearer)) -> str:
    """Validate the bearer token against the API_SECRET_TOKEN env var."""
    expected = os.environ.get("API_SECRET_TOKEN")
    if not expected:
        raise HTTPException(status_code=500, detail="Server misconfiguration: API_SECRET_TOKEN not set")
    if credentials.credentials != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return credentials.credentials


@app.get("/ping")
def ping():
    # Intentionally unauthenticated: this is a public health-check/liveness-probe
    # endpoint. It accepts no user input and returns only a static response, so
    # there is no path-traversal, IDOR, or injection surface.
    return {"message": "pong"}


@app.get("/files/{file_path:path}")
async def get_file(file_path: str, _token: str = Security(_verify_token)):
    # Reject path traversal sequences before forwarding to the storage service.
    if ".." in file_path.split("/"):
        raise HTTPException(status_code=400, detail="Invalid file path")
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://storage-service:8080/files/{file_path}") as resp:
            content = await resp.read()
            return Response(content=content, media_type=resp.content_type)


@app.get("/users/{user_id}")
async def get_user(user_id: int, _token: str = Security(_verify_token)):
    """Fetch a user by ID — requires a valid bearer token."""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://user-service:8080/users/{user_id}") as resp:
            return await resp.json()
