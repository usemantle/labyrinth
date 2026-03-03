"""
Unit tests for the FastAPI codebase plugin.

Verifies HTTP method detection, route path extraction, and full route
path resolution from include_router + APIRouter prefixes.
"""

import uuid
from pathlib import Path

import pytest

from src.graph.graph_models import RelationType
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from src.graph.loaders.codebase.plugins.fastapi_plugin import FastAPIPlugin

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

# ── Synthetic codebase ────────────────────────────────────────────────

MAIN_PY = """\
from fastapi import FastAPI
from myapp.api.users import router as users_router

app = FastAPI()
app.include_router(users_router, prefix="/v1")
"""

API_USERS_PY = """\
from fastapi import APIRouter
from myapp.services.user_svc import get_user_by_id

router = APIRouter(prefix="/users")

def format_response(data):
    return {"status": "ok", "data": data}

@router.get("/{user_id}")
def get_user(user_id: str):
    raw = get_user_by_id(user_id)
    return format_response(raw)
"""

SERVICES_USER_SVC_PY = """\
def get_user_by_id(user_id: str):
    return {"id": user_id, "name": "Test User"}
"""


def _make_fastapi_app(tmp_path: Path) -> Path:
    """Create the synthetic codebase on disk."""
    root = tmp_path / "myapp"
    root.mkdir()
    (root / "__init__.py").write_text("")

    (root / "main.py").write_text(MAIN_PY)

    api_dir = root / "api"
    api_dir.mkdir()
    (api_dir / "__init__.py").write_text("")
    (api_dir / "users.py").write_text(API_USERS_PY)

    svc_dir = root / "services"
    svc_dir.mkdir()
    (svc_dir / "__init__.py").write_text("")
    (svc_dir / "user_svc.py").write_text(SERVICES_USER_SVC_PY)

    return root


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture()
def graph_result(tmp_path):
    """Scan the synthetic codebase and return (nodes, edges)."""
    root = _make_fastapi_app(tmp_path)
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID,
        plugins=[FastAPIPlugin()],
    )
    return loader.load(str(root))


# ── Helpers ───────────────────────────────────────────────────────────


def _find_node(nodes, **metadata_match):
    """Find a node whose metadata contains all key-value pairs."""
    for node in nodes:
        if all(node.metadata.get(k) == v for k, v in metadata_match.items()):
            return node
    keys = ", ".join(f"{k}={v!r}" for k, v in metadata_match.items())
    pytest.fail(f"No node found with metadata: {keys}")


# ── Tests: FastAPI entrypoint detection ───────────────────────────────


def test_fastapi_http_method_detected(graph_result):
    """get_user() is decorated with @router.get -- should have http_method metadata."""
    nodes, _ = graph_result
    endpoint = _find_node(nodes, function_name="get_user")

    assert endpoint.metadata.get("http_method") == "GET"
    assert endpoint.metadata.get("api_framework") == "fastapi"


def test_fastapi_route_path_from_decorator(graph_result):
    """The local route path from the decorator should be captured."""
    nodes, _ = graph_result
    endpoint = _find_node(nodes, function_name="get_user")

    assert endpoint.metadata.get("route_path") == "/{user_id}"


def test_fastapi_full_route_path(graph_result):
    """The full route path should combine:
    include_router prefix (/v1) + APIRouter prefix (/users) + decorator path (/{user_id})
    = /v1/users/{user_id}"""
    nodes, _ = graph_result
    endpoint = _find_node(nodes, function_name="get_user")

    assert endpoint.metadata.get("full_route_path") == "/v1/users/{user_id}"


# ── Tests: Non-endpoint functions should NOT have API metadata ────────


def test_non_endpoint_has_no_api_metadata(graph_result):
    """format_response() is not decorated -- should have no API metadata."""
    nodes, _ = graph_result
    func = _find_node(nodes, function_name="format_response")

    assert func.metadata.get("http_method") is None
    assert func.metadata.get("route_path") is None


def test_service_function_has_no_api_metadata(graph_result):
    """get_user_by_id() in the service file should have no API metadata."""
    nodes, _ = graph_result
    func = _find_node(nodes, function_name="get_user_by_id")

    assert func.metadata.get("http_method") is None
