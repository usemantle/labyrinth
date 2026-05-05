"""
Unit tests for the FastAPI codebase plugin.

Verifies HTTP method detection, route path extraction, full route
path resolution from include_router + APIRouter prefixes, and
auth scheme detection/propagation.
"""

import uuid
from pathlib import Path

import pytest

from labyrinth.graph.graph_models import NodeMetadataKey
from labyrinth.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from labyrinth.graph.loaders.codebase.plugins.fastapi_plugin import FastAPIPlugin

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

NK = NodeMetadataKey

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


# ── Tests: Auth scheme detection ──────────────────────────────────────


def test_fastapi_httpbearer_auth_detected(tmp_path):
    """HTTPBearer scheme should be detected via Depends(security_var)."""
    root = tmp_path / "myapp"
    root.mkdir()
    (root / "__init__.py").write_text("")
    (root / "main.py").write_text(
        'from fastapi import FastAPI\n'
        'from myapp.routes import router\n'
        'app = FastAPI()\n'
        'app.include_router(router)\n'
    )
    routes = root / "routes.py"
    routes.write_text(
        'from fastapi import APIRouter, Depends\n'
        'from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials\n'
        '\n'
        'security_optional = HTTPBearer(auto_error=False)\n'
        '\n'
        'router = APIRouter()\n'
        '\n'
        '@router.get("/users")\n'
        'async def list_users(\n'
        '    credentials: HTTPAuthorizationCredentials | None = Depends(security_optional),\n'
        '):\n'
        '    return []\n'
    )

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[FastAPIPlugin()],
    )
    nodes, _ = loader.load(str(root))

    ep = _find_node(nodes, function_name="list_users")
    assert ep.metadata.get(NK.AUTH_SCHEME) == "HTTPBearer"
    assert ep.metadata.get(NK.AUTH_SCHEME_VAR) == "security_optional"


def test_fastapi_apikeyheader_auth_detected(tmp_path):
    """APIKeyHeader scheme should be detected via Depends(api_key_header)."""
    root = tmp_path / "myapp"
    root.mkdir()
    (root / "__init__.py").write_text("")
    (root / "main.py").write_text(
        'from fastapi import FastAPI\n'
        'from myapp.routes import router\n'
        'app = FastAPI()\n'
        'app.include_router(router)\n'
    )
    routes = root / "routes.py"
    routes.write_text(
        'from fastapi import APIRouter, Depends\n'
        'from fastapi.security import APIKeyHeader\n'
        '\n'
        'api_key_header = APIKeyHeader(name="X-API-Key")\n'
        '\n'
        'router = APIRouter()\n'
        '\n'
        '@router.get("/data")\n'
        'async def get_data(\n'
        '    api_key: str = Depends(api_key_header),\n'
        '):\n'
        '    return {}\n'
    )

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[FastAPIPlugin()],
    )
    nodes, _ = loader.load(str(root))

    ep = _find_node(nodes, function_name="get_data")
    assert ep.metadata.get(NK.AUTH_SCHEME) == "APIKeyHeader"
    assert ep.metadata.get(NK.AUTH_SCHEME_VAR) == "api_key_header"


def test_fastapi_oauth2_password_bearer_detected(tmp_path):
    """OAuth2PasswordBearer scheme should be detected."""
    root = tmp_path / "myapp"
    root.mkdir()
    (root / "__init__.py").write_text("")
    (root / "main.py").write_text(
        'from fastapi import FastAPI\n'
        'from myapp.routes import router\n'
        'app = FastAPI()\n'
        'app.include_router(router)\n'
    )
    routes = root / "routes.py"
    routes.write_text(
        'from fastapi import APIRouter, Depends\n'
        'from fastapi.security import OAuth2PasswordBearer\n'
        '\n'
        'oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")\n'
        '\n'
        'router = APIRouter()\n'
        '\n'
        '@router.get("/me")\n'
        'async def get_me(\n'
        '    token: str = Depends(oauth2_scheme),\n'
        '):\n'
        '    return {}\n'
    )

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[FastAPIPlugin()],
    )
    nodes, _ = loader.load(str(root))

    ep = _find_node(nodes, function_name="get_me")
    assert ep.metadata.get(NK.AUTH_SCHEME) == "OAuth2PasswordBearer"
    assert ep.metadata.get(NK.AUTH_SCHEME_VAR) == "oauth2_scheme"


def test_fastapi_router_level_auth_propagated(tmp_path):
    """Auth from APIRouter(dependencies=[Depends(var)]) should propagate to all endpoints."""
    root = tmp_path / "myapp"
    root.mkdir()
    (root / "__init__.py").write_text("")
    (root / "main.py").write_text(
        'from fastapi import FastAPI\n'
        'from myapp.routes import router\n'
        'app = FastAPI()\n'
        'app.include_router(router)\n'
    )
    routes = root / "routes.py"
    routes.write_text(
        'from fastapi import APIRouter, Depends\n'
        'from fastapi.security import HTTPBearer\n'
        '\n'
        'auth = HTTPBearer()\n'
        '\n'
        'router = APIRouter(dependencies=[Depends(auth)])\n'
        '\n'
        '@router.get("/protected")\n'
        'async def protected_endpoint():\n'
        '    return {"ok": True}\n'
        '\n'
        '@router.post("/create")\n'
        'async def create_thing():\n'
        '    return {"created": True}\n'
    )

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[FastAPIPlugin()],
    )
    nodes, _ = loader.load(str(root))

    ep1 = _find_node(nodes, function_name="protected_endpoint")
    assert ep1.metadata.get(NK.AUTH_SCHEME) == "HTTPBearer"

    ep2 = _find_node(nodes, function_name="create_thing")
    assert ep2.metadata.get(NK.AUTH_SCHEME) == "HTTPBearer"


def test_fastapi_no_auth_when_no_security(tmp_path):
    """Endpoints without any security primitives should have no auth_scheme."""
    root = tmp_path / "myapp"
    root.mkdir()
    (root / "__init__.py").write_text("")
    (root / "main.py").write_text(
        'from fastapi import FastAPI\n'
        'from myapp.routes import router\n'
        'app = FastAPI()\n'
        'app.include_router(router)\n'
    )
    routes = root / "routes.py"
    routes.write_text(
        'from fastapi import APIRouter\n'
        '\n'
        'router = APIRouter()\n'
        '\n'
        '@router.get("/public")\n'
        'async def public_endpoint():\n'
        '    return {"public": True}\n'
    )

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[FastAPIPlugin()],
    )
    nodes, _ = loader.load(str(root))

    ep = _find_node(nodes, function_name="public_endpoint")
    assert ep.metadata.get(NK.AUTH_SCHEME) is None
    assert ep.metadata.get(NK.AUTH_SCHEME_VAR) is None


def test_fastapi_existing_route_detection_unchanged(graph_result):
    """The original route detection behavior should be preserved."""
    nodes, _ = graph_result
    endpoint = _find_node(nodes, function_name="get_user")

    assert endpoint.metadata.get(NK.HTTP_METHOD) == "GET"
    assert endpoint.metadata.get(NK.ROUTE_PATH) == "/{user_id}"
    assert endpoint.metadata.get(NK.API_FRAMEWORK) == "fastapi"
    assert endpoint.metadata.get(NK.ROUTER_VARIABLE) == "router"
    assert endpoint.metadata.get(NK.FULL_ROUTE_PATH) == "/v1/users/{user_id}"
