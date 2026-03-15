"""Minimal FastAPI app for testing Labyrinth ingestors."""

from fastapi import FastAPI

app = FastAPI(title="labyrinth-testapp")


@app.get("/ping")
def ping():
    return {"message": "pong"}
