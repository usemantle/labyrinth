"""Minimal FastAPI app for testing Labyrinth ingestors."""

import aiohttp
import boto3
from fastapi import FastAPI
from fastapi.responses import Response
from s3_wrapper import S3Wrapper

app = FastAPI(title="labyrinth-testapp")

BUCKET = "nexus-sec-test"
COUNTER_KEY = "times_called.txt"

_s3_wrapper = S3Wrapper(bucket=BUCKET)


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


@app.post("/counter/direct")
def increment_counter_direct():
    """Increment a counter in S3 using a direct boto3 client."""
    s3 = boto3.client("s3", region_name="us-east-1")

    # Read current value
    try:
        resp = s3.get_object(Bucket=BUCKET, Key=COUNTER_KEY)
        count = int(resp["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        count = 0

    count += 1
    s3.put_object(Bucket=BUCKET, Key=COUNTER_KEY, Body=str(count).encode("utf-8"))

    return {"count": count, "method": "direct"}


@app.post("/counter/wrapped")
def increment_counter_wrapped():
    """Increment a counter in S3 using the S3Wrapper (indirection)."""
    body = _s3_wrapper.read(COUNTER_KEY)
    count = int(body) if body is not None else 0

    count += 1
    _s3_wrapper.write(COUNTER_KEY, str(count))

    return {"count": count, "method": "wrapped"}
