"""
End-to-end tests for cross-file import resolution, call graph
construction, and FastAPI entrypoint detection.

Uses three synthetic Python files:
- main.py:            Server entrypoint with include_router
- api/users.py:       API endpoint with @router.get decorator, calls service
- services/user_svc.py: Service function imported by the API handler

Asserts:
1. CODE_TO_CODE edge from get_user → get_user_by_id (cross-file call)
2. CODE_TO_CODE edge from get_user → format_response (same-file call)
3. API entrypoint metadata on get_user (http_method, route_path, full_route_path)
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


def _make_app(tmp_path: Path) -> Path:
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
    root = _make_app(tmp_path)
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


def _find_edges(edges, from_urn=None, to_urn=None, relation_type=None):
    """Find edges matching the given criteria."""
    result = []
    for edge in edges:
        if from_urn and str(edge.from_urn) != str(from_urn):
            continue
        if to_urn and str(edge.to_urn) != str(to_urn):
            continue
        if relation_type and edge.relation_type != relation_type:
            continue
        result.append(edge)
    return result


# ── Tests: Cross-file call graph (CODE_TO_CODE) ──────────────────────


def test_cross_file_function_call(graph_result):
    """get_user() calls get_user_by_id() which is imported from another file.
    This should produce a CODE_TO_CODE edge."""
    nodes, edges = graph_result

    caller = _find_node(nodes, function_name="get_user")
    callee = _find_node(nodes, function_name="get_user_by_id")

    code_to_code = _find_edges(
        edges,
        from_urn=caller.urn,
        to_urn=callee.urn,
        relation_type=RelationType.CODE_TO_CODE,
    )
    assert len(code_to_code) == 1, (
        f"Expected 1 CODE_TO_CODE edge from get_user → get_user_by_id, "
        f"found {len(code_to_code)}"
    )
    assert code_to_code[0].metadata.get("call_type") == "function_call"


def test_same_file_function_call(graph_result):
    """get_user() calls format_response() defined in the same file.
    This should also produce a CODE_TO_CODE edge."""
    nodes, edges = graph_result

    caller = _find_node(nodes, function_name="get_user")
    callee = _find_node(nodes, function_name="format_response")

    code_to_code = _find_edges(
        edges,
        from_urn=caller.urn,
        to_urn=callee.urn,
        relation_type=RelationType.CODE_TO_CODE,
    )
    assert len(code_to_code) == 1, (
        f"Expected 1 CODE_TO_CODE edge from get_user → format_response, "
        f"found {len(code_to_code)}"
    )


# ── Tests: FastAPI entrypoint detection ───────────────────────────────


def test_fastapi_http_method_detected(graph_result):
    """get_user() is decorated with @router.get — should have http_method metadata."""
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
    """format_response() is not decorated — should have no API metadata."""
    nodes, _ = graph_result
    func = _find_node(nodes, function_name="format_response")

    assert func.metadata.get("http_method") is None
    assert func.metadata.get("route_path") is None


def test_service_function_has_no_api_metadata(graph_result):
    """get_user_by_id() in the service file should have no API metadata."""
    nodes, _ = graph_result
    func = _find_node(nodes, function_name="get_user_by_id")

    assert func.metadata.get("http_method") is None
