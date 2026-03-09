"""
End-to-end tests for Python cross-file import resolution and call graph
construction.

Uses three synthetic Python files:
- main.py:            Server entrypoint with include_router
- api/users.py:       API endpoint with @router.get decorator, calls service
- services/user_svc.py: Service function imported by the API handler

Asserts:
1. CODE_TO_CODE edge from get_user -> get_user_by_id (cross-file call)
2. CODE_TO_CODE edge from get_user -> format_response (same-file call)
"""

import uuid
from pathlib import Path

import pytest

from src.graph.graph_models import NodeMetadataKey
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


def _find_edges(edges, from_urn=None, to_urn=None, edge_type=None):
    """Find edges matching the given criteria."""
    result = []
    for edge in edges:
        if from_urn and str(edge.from_urn) != str(from_urn):
            continue
        if to_urn and str(edge.to_urn) != str(to_urn):
            continue
        if edge_type and edge.edge_type != edge_type:
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
        edge_type="calls",
    )
    assert len(code_to_code) == 1, (
        f"Expected 1 CODE_TO_CODE edge from get_user -> get_user_by_id, "
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
        edge_type="calls",
    )
    assert len(code_to_code) == 1, (
        f"Expected 1 CODE_TO_CODE edge from get_user -> format_response, "
        f"found {len(code_to_code)}"
    )


# ── Tests: Same-file helper call inside assertion ────────────────────


def test_python_same_file_call_in_assertion(tmp_path):
    """A function that calls a same-file helper inside an assert statement
    should still produce a CODE_TO_CODE edge."""
    root = tmp_path / "myapp"
    root.mkdir()
    (root / "__init__.py").write_text("")
    (root / "vault.py").write_text(
        "def is_safe_name(name: str) -> bool:\n"
        "    return bool(name) and name != '.' and '/' not in name\n"
        "\n"
        "def vault_path(name: str) -> str:\n"
        "    assert is_safe_name(name), f'unsafe: {name}'\n"
        "    return f'.vault.{name}.toml'\n"
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, edges = loader.load(str(root))

    caller = _find_node(nodes, function_name="vault_path")
    callee = _find_node(nodes, function_name="is_safe_name")

    code_to_code = _find_edges(
        edges,
        from_urn=caller.urn,
        to_urn=callee.urn,
        edge_type="calls",
    )
    assert len(code_to_code) == 1, (
        f"Expected 1 CODE_TO_CODE edge from vault_path -> is_safe_name, "
        f"found {len(code_to_code)}"
    )


# ── Tests: Class instantiation (InstantiatesEdge) ────────────────────


def test_cross_file_class_instantiation(tmp_path):
    """A function that imports and instantiates a class from another file
    should produce an InstantiatesEdge."""
    root = tmp_path / "myapp"
    root.mkdir()
    (root / "__init__.py").write_text("")
    (root / "models.py").write_text(
        "class User:\n"
        "    def __init__(self, name: str):\n"
        "        self.name = name\n"
    )
    (root / "service.py").write_text(
        "from myapp.models import User\n"
        "\n"
        "def create_user(name: str):\n"
        "    return User(name)\n"
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, edges = loader.load(str(root))

    caller = _find_node(nodes, function_name="create_user")
    target_class = _find_node(nodes, class_name="User")

    instantiates = [
        e for e in edges
        if str(e.from_urn) == str(caller.urn)
        and str(e.to_urn) == str(target_class.urn)
        and e.edge_type == "instantiates"
    ]
    assert len(instantiates) == 1, (
        f"Expected 1 instantiates edge from create_user -> User, "
        f"found {len(instantiates)}"
    )
    assert instantiates[0].metadata.get("call_type") == "class_instantiation"


def test_same_file_class_instantiation(tmp_path):
    """A function that instantiates a class defined in the same file
    should produce an InstantiatesEdge."""
    root = tmp_path / "myapp"
    root.mkdir()
    (root / "__init__.py").write_text("")
    (root / "service.py").write_text(
        "class Config:\n"
        "    def __init__(self):\n"
        "        self.debug = False\n"
        "\n"
        "def get_config():\n"
        "    return Config()\n"
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, edges = loader.load(str(root))

    caller = _find_node(nodes, function_name="get_config")
    target_class = _find_node(nodes, class_name="Config")

    instantiates = [
        e for e in edges
        if str(e.from_urn) == str(caller.urn)
        and str(e.to_urn) == str(target_class.urn)
        and e.edge_type == "instantiates"
    ]
    assert len(instantiates) == 1, (
        f"Expected 1 instantiates edge from get_config -> Config, "
        f"found {len(instantiates)}"
    )


# ── Tests: Dependency linking ─────────────────────────────────────────


def test_link_dependencies_creates_depends_on_edges(tmp_path):
    """PythonAnalyzer.link_dependencies() should create DependsOn edges
    from files to dependency nodes based on imports."""
    from src.graph.graph_models import URN
    from src.graph.loaders.codebase.codebase_loader import PostProcessContext
    from src.graph.loaders.codebase.resolvers.python import PythonAnalyzer
    from src.graph.nodes.dependency_node import DependencyNode
    from src.graph.nodes.file_node import FileNode

    root = tmp_path / "myapp"
    root.mkdir()
    (root / "__init__.py").write_text("")
    (root / "service.py").write_text("import requests\n\ndef fetch():\n    return requests.get('http://example.com')\n")

    # Create nodes that would normally come from Phase 1 + UvPlugin
    file_node = FileNode.create(
        ORG_ID,
        URN("urn:local:codebase:::myapp/service.py"),
        URN("urn:local:codebase:::myapp"),
        file_path="service.py",
        language="python",
    )
    dep_node = DependencyNode.create(
        ORG_ID,
        URN("urn:pypi:package:::requests"),
        package_name="requests",
        package_version="2.31.0",
    )
    nodes = [file_node, dep_node]
    edges = []

    ctx = PostProcessContext(
        root_path=root,
        root_name="myapp",
        organization_id=ORG_ID,
        file_sources={"service.py": (root / "service.py").read_text()},
        file_languages={"service.py": "python"},
        build_urn=lambda *parts: URN(f"urn:local:codebase:::{'/'.join(parts)}"),
    )

    analyzer = PythonAnalyzer()
    nodes, edges = analyzer.link_dependencies(nodes, edges, ctx.file_sources, ctx)

    depends_on = [e for e in edges if e.edge_type == "depends_on"]
    assert len(depends_on) == 1
    assert str(depends_on[0].from_urn) == str(file_node.urn)
    assert str(depends_on[0].to_urn) == str(dep_node.urn)
    assert depends_on[0].metadata.get("import_name") == "requests"
