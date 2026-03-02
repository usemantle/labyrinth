"""
Unit tests for codebase loaders, plugins, and orchestrator.

All filesystem access and git operations are mocked — no real
repositories required.
"""

import uuid
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from src.graph.graph_models import (
    EdgeMetadataKey,
    Node,
    NodeMetadata,
    NodeMetadataKey,
    RelationType,
    URN,
)
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from src.graph.loaders.codebase.git_codebase_loader import GitCodebaseLoader
from src.graph.loaders.codebase.plugins import Boto3S3Plugin, CodebasePlugin, SQLAlchemyPlugin
from scripts.build_graph import stitch_code_to_data

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

NK = NodeMetadataKey
EK = EdgeMetadataKey


# ── Helpers ────────────────────────────────────────────────────────────


def _make_tmp_repo(tmp_path: Path) -> Path:
    """Create a minimal fake repo on disk."""
    repo = tmp_path / "my-repo"
    repo.mkdir()

    # Python file with a class and top-level function
    (repo / "app.py").write_text(
        'class UserService:\n'
        '    def get_user(self, user_id):\n'
        '        return {"id": user_id}\n'
        '\n'
        'def health_check():\n'
        '    return "ok"\n'
    )

    # Python file with decorated class
    models = repo / "models"
    models.mkdir()
    (models / "__init__.py").write_text("")
    (models / "user.py").write_text(
        'from dataclasses import dataclass\n'
        '\n'
        '@dataclass\n'
        'class User:\n'
        '    email: str\n'
        '    name: str\n'
    )

    # JavaScript file
    (repo / "index.js").write_text(
        'class App {\n'
        '    start() {\n'
        '        console.log("started");\n'
        '    }\n'
        '}\n'
        '\n'
        'function main() {\n'
        '    new App().start();\n'
        '}\n'
    )

    # Empty Python file (should still get a file node)
    (repo / "empty.py").write_text("")

    # Nested class
    (repo / "nested.py").write_text(
        'class Outer:\n'
        '    class Inner:\n'
        '        def inner_method(self):\n'
        '            pass\n'
        '\n'
        '    def outer_method(self):\n'
        '        pass\n'
    )

    # Should be excluded: node_modules
    nm = repo / "node_modules" / "pkg"
    nm.mkdir(parents=True)
    (nm / "index.js").write_text('function hidden() {}')

    # Non-code file (should be excluded)
    (repo / "README.md").write_text("# My Repo")

    return repo


# ── FileSystemCodebaseLoader tests ─────────────────────────────────────


@pytest.fixture()
def fs_result(tmp_path):
    """Run FileSystemCodebaseLoader on a temporary repo."""
    repo = _make_tmp_repo(tmp_path)
    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    return loader.load(str(repo))


def test_fs_loader_urn_scheme(fs_result):
    nodes, _ = fs_result
    root = nodes[0]
    assert str(root.urn).startswith("urn:local:codebase:localhost:_:")


def test_fs_codebase_root_node(fs_result):
    nodes, _ = fs_result
    root = nodes[0]
    assert root.parent_urn is None
    assert root.metadata[NK.REPO_NAME] == "my-repo"
    assert root.metadata[NK.FILE_COUNT] > 0


def test_fs_file_nodes(fs_result):
    nodes, _ = fs_result
    files = [n for n in nodes if NK.FILE_PATH in n.metadata and NK.CLASS_NAME not in n.metadata]
    file_paths = {n.metadata[NK.FILE_PATH] for n in files}

    assert "app.py" in file_paths
    assert "index.js" in file_paths
    assert "models/user.py" in file_paths
    assert "models/__init__.py" in file_paths
    assert "nested.py" in file_paths
    assert "empty.py" in file_paths


def test_fs_excludes_node_modules(fs_result):
    nodes, _ = fs_result
    file_paths = {n.metadata.get(NK.FILE_PATH, "") for n in nodes}
    assert not any("node_modules" in p for p in file_paths)


def test_fs_excludes_non_code_files(fs_result):
    nodes, _ = fs_result
    file_paths = {n.metadata.get(NK.FILE_PATH, "") for n in nodes}
    assert "README.md" not in file_paths


def test_fs_python_class_extraction(fs_result):
    nodes, _ = fs_result
    classes = [n for n in nodes if NK.CLASS_NAME in n.metadata]
    class_names = {n.metadata[NK.CLASS_NAME] for n in classes}
    assert "UserService" in class_names
    assert "User" in class_names  # from decorated @dataclass


def test_fs_python_method_extraction(fs_result):
    nodes, _ = fs_result
    methods = [n for n in nodes if n.metadata.get(NK.IS_METHOD) is True]
    method_names = {n.metadata[NK.FUNCTION_NAME] for n in methods}
    assert "get_user" in method_names


def test_fs_python_top_level_function(fs_result):
    nodes, _ = fs_result
    funcs = [
        n for n in nodes
        if n.metadata.get(NK.FUNCTION_NAME) == "health_check"
    ]
    assert len(funcs) == 1
    assert funcs[0].metadata[NK.IS_METHOD] is False


def test_fs_javascript_class_extraction(fs_result):
    nodes, _ = fs_result
    classes = [n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "App"]
    assert len(classes) == 1


def test_fs_javascript_method_extraction(fs_result):
    nodes, _ = fs_result
    methods = [
        n for n in nodes
        if n.metadata.get(NK.FUNCTION_NAME) == "start"
        and n.metadata.get(NK.IS_METHOD) is True
    ]
    assert len(methods) == 1


def test_fs_javascript_function_extraction(fs_result):
    nodes, _ = fs_result
    funcs = [
        n for n in nodes
        if n.metadata.get(NK.FUNCTION_NAME) == "main"
    ]
    assert len(funcs) == 1


def test_fs_nested_class(fs_result):
    nodes, _ = fs_result
    inner = [n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Inner"]
    assert len(inner) == 1
    # Inner's parent should be Outer
    outer = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Outer")
    assert inner[0].parent_urn == outer.urn


def test_fs_nested_method(fs_result):
    nodes, _ = fs_result
    inner_method = [
        n for n in nodes
        if n.metadata.get(NK.FUNCTION_NAME) == "inner_method"
    ]
    assert len(inner_method) == 1
    inner_class = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Inner")
    assert inner_method[0].parent_urn == inner_class.urn


def test_fs_contains_edge_counts(fs_result):
    _, edges = fs_result
    contains = [e for e in edges if e.relation_type == RelationType.CONTAINS]
    # Every node except root has a CONTAINS edge pointing to it
    assert len(contains) == len([n for n, _ in [(None, None)]] * 0) or len(contains) > 0
    # More specifically: edges = nodes - 1 (root has no incoming edge)
    nodes, _ = fs_result
    assert len(contains) == len(nodes) - 1


def test_fs_all_edges_are_contains(fs_result):
    """Phase 1: only CONTAINS edges, no CODE_TO_DATA yet."""
    _, edges = fs_result
    for edge in edges:
        assert edge.relation_type == RelationType.CONTAINS


def test_fs_parent_urn_file_to_root(fs_result):
    nodes, _ = fs_result
    root = nodes[0]
    # File nodes have file_path but NOT class_name, function_name, or is_method
    files = [
        n for n in nodes
        if NK.FILE_PATH in n.metadata
        and NK.CLASS_NAME not in n.metadata
        and NK.FUNCTION_NAME not in n.metadata
    ]
    for f in files:
        assert f.parent_urn == root.urn


def test_fs_parent_urn_class_to_file(fs_result):
    nodes, _ = fs_result
    user_service = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "UserService")
    app_file = next(
        n for n in nodes
        if n.metadata.get(NK.FILE_PATH) == "app.py" and NK.CLASS_NAME not in n.metadata
    )
    assert user_service.parent_urn == app_file.urn


def test_fs_parent_urn_method_to_class(fs_result):
    nodes, _ = fs_result
    get_user = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "get_user")
    user_service = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "UserService")
    assert get_user.parent_urn == user_service.urn


def test_fs_file_metadata(fs_result):
    nodes, _ = fs_result
    app_file = next(
        n for n in nodes
        if n.metadata.get(NK.FILE_PATH) == "app.py" and NK.CLASS_NAME not in n.metadata
    )
    assert app_file.metadata[NK.LANGUAGE] == "python"
    assert app_file.metadata[NK.SIZE_BYTES] > 0


def test_fs_class_metadata(fs_result):
    nodes, _ = fs_result
    user_service = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "UserService")
    assert user_service.metadata[NK.FILE_PATH] == "app.py"
    assert NK.START_LINE in user_service.metadata
    assert NK.END_LINE in user_service.metadata


def test_fs_function_metadata(fs_result):
    nodes, _ = fs_result
    health = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "health_check")
    assert health.metadata[NK.FILE_PATH] == "app.py"
    assert health.metadata[NK.IS_METHOD] is False
    assert NK.START_LINE in health.metadata


def test_fs_empty_file(fs_result):
    """Empty files get a file node but no class/function nodes."""
    nodes, _ = fs_result
    empty_file = next(
        n for n in nodes
        if n.metadata.get(NK.FILE_PATH) == "empty.py" and NK.CLASS_NAME not in n.metadata
    )
    assert empty_file is not None
    # No children from empty file
    empty_urn = empty_file.urn
    children = [n for n in nodes if n.parent_urn == empty_urn]
    assert len(children) == 0


def test_fs_org_isolation(fs_result):
    nodes, edges = fs_result
    for node in nodes:
        assert node.organization_id == ORG_ID
    for edge in edges:
        assert edge.organization_id == ORG_ID


def test_fs_edge_uuids_deterministic(tmp_path):
    """Running the loader twice produces identical edge UUIDs."""
    repo = _make_tmp_repo(tmp_path)
    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)

    _, edges1 = loader.load(str(repo))
    _, edges2 = loader.load(str(repo))

    uuids1 = {e.uuid for e in edges1}
    uuids2 = {e.uuid for e in edges2}
    assert uuids1 == uuids2


# ── Decorated class handling ───────────────────────────────────────────


def test_fs_decorated_class(fs_result):
    """@dataclass decorated class is still discovered."""
    nodes, _ = fs_result
    user = [n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "User"]
    assert len(user) == 1


# ── GitCodebaseLoader tests ─────────────────────────────────────────


@pytest.fixture()
def git_result(tmp_path):
    """Run GitCodebaseLoader on a temporary repo."""
    repo = _make_tmp_repo(tmp_path)
    with patch(
        "src.graph.loaders.codebase.git_codebase_loader.GitCodebaseLoader._get_head_commit"
    ) as mock_commit:
        mock_commit.return_value = "abc123def456"
        loader = GitCodebaseLoader(
            organization_id=ORG_ID,
            repo_url="https://github.com/acme/my-service.git",
            repo_hostname="github.com",
            repo_path="acme/my-service",
        )
        return loader.load(str(repo))


def test_git_urn_scheme(git_result):
    nodes, _ = git_result
    root = nodes[0]
    assert str(root.urn) == "urn:git:repo:github.com:_:my-service"


def test_git_root_metadata(git_result):
    nodes, _ = git_result
    root = nodes[0]
    assert root.metadata[NK.REPO_NAME] == "my-service"
    assert root.metadata[NK.SCANNED_COMMIT] == "abc123def456"
    assert root.metadata[NK.REPO_URL] == "https://github.com/acme/my-service.git"


def test_git_file_urn(git_result):
    nodes, _ = git_result
    app = next(
        n for n in nodes
        if n.metadata.get(NK.FILE_PATH) == "app.py" and NK.CLASS_NAME not in n.metadata
    )
    assert str(app.urn) == "urn:git:repo:github.com:_:my-service/app.py"


def test_git_class_urn(git_result):
    nodes, _ = git_result
    user_service = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "UserService")
    assert str(user_service.urn) == "urn:git:repo:github.com:_:my-service/app.py/UserService"


def test_git_method_urn(git_result):
    nodes, _ = git_result
    get_user = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "get_user")
    assert str(get_user.urn) == "urn:git:repo:github.com:_:my-service/app.py/UserService/get_user"


def test_git_uses_repo_name_not_dir_name(git_result):
    """Root name is repo_path last segment, not the tmp dir name."""
    nodes, _ = git_result
    root = nodes[0]
    assert "my-service" in str(root.urn)
    assert root.metadata[NK.REPO_NAME] == "my-service"


# ── Language detection ─────────────────────────────────────────────────


def test_language_detection():
    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    assert loader._detect_language(Path("test.py")) == "python"
    assert loader._detect_language(Path("test.js")) == "javascript"
    assert loader._detect_language(Path("test.ts")) == "typescript"
    assert loader._detect_language(Path("test.java")) == "java"
    assert loader._detect_language(Path("test.md")) is None
    assert loader._detect_language(Path("test.txt")) is None


# ── Base class extraction ──────────────────────────────────────────────


def test_base_classes_extracted(tmp_path):
    """Python inheritance is captured in class metadata."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "models.py").write_text(
        'class Base:\n'
        '    pass\n'
        '\n'
        'class User(Base):\n'
        '    pass\n'
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, _ = loader.load(str(repo))

    user = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "User")
    assert user.metadata.get(NK.BASE_CLASSES) == ["Base"]

    base = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Base")
    assert NK.BASE_CLASSES not in base.metadata  # No bases


# ── Overloaded method disambiguation ──────────────────────────────────


def test_overloaded_method_disambiguation(tmp_path):
    """Java-style overloaded methods get ordinal suffixes."""
    repo = tmp_path / "repo"
    repo.mkdir()
    # TypeScript supports method overloading via declarations
    (repo / "service.ts").write_text(
        'class Service {\n'
        '    handle() { return 1; }\n'
        '    handle() { return 2; }\n'
        '}\n'
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, _ = loader.load(str(repo))

    handles = [n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "handle"]
    # Both should exist with different URNs
    assert len(handles) == 2
    urns = {str(n.urn) for n in handles}
    assert len(urns) == 2  # Different URNs due to disambiguation


# ── Plugin system tests ───────────────────────────────────────────────


class _TrackingPlugin(CodebasePlugin):
    """Test plugin that records what it was called with."""

    def __init__(self):
        self.class_calls = []
        self.function_calls = []

    def on_class_node(self, node, class_body_source, language):
        self.class_calls.append((node.metadata.get(NK.CLASS_NAME), language))
        node.metadata[NK.ORM_TABLE] = "__tracked__"
        return node

    def on_function_node(self, node, function_source, language):
        self.function_calls.append((node.metadata.get(NK.FUNCTION_NAME), language))
        node.metadata[NK.ORM_TABLE] = "__tracked__"
        return node


def test_plugin_receives_class_nodes(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "models.py").write_text(
        'class User:\n'
        '    name: str\n'
    )

    plugin = _TrackingPlugin()
    loader = FileSystemCodebaseLoader(organization_id=ORG_ID, plugins=[plugin])
    nodes, _ = loader.load(str(repo))

    assert len(plugin.class_calls) == 1
    assert plugin.class_calls[0] == ("User", "python")

    user = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "User")
    assert user.metadata[NK.ORM_TABLE] == "__tracked__"


def test_plugin_receives_function_nodes(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "utils.py").write_text(
        'def hello():\n'
        '    return "hi"\n'
    )

    plugin = _TrackingPlugin()
    loader = FileSystemCodebaseLoader(organization_id=ORG_ID, plugins=[plugin])
    nodes, _ = loader.load(str(repo))

    assert len(plugin.function_calls) == 1
    assert plugin.function_calls[0] == ("hello", "python")


def test_plugin_skips_non_python_for_language_check(tmp_path):
    """Plugin receives the correct language for JS files."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "app.js").write_text(
        'class App {}\n'
        'function main() {}\n'
    )

    plugin = _TrackingPlugin()
    loader = FileSystemCodebaseLoader(organization_id=ORG_ID, plugins=[plugin])
    loader.load(str(repo))

    assert all(lang == "javascript" for _, lang in plugin.class_calls)
    assert all(lang == "javascript" for _, lang in plugin.function_calls)


def test_multiple_plugins_chain(tmp_path):
    """Multiple plugins are called in order and each sees the enriched node."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "m.py").write_text('class Foo:\n    pass\n')

    class PluginA(CodebasePlugin):
        def on_class_node(self, node, src, lang):
            node.metadata[NK.ORM_TABLE] = "test_chain"
            return node

    class PluginB(CodebasePlugin):
        def on_class_node(self, node, src, lang):
            node.metadata[NK.ORM_FRAMEWORK] = node.metadata.get(NK.ORM_TABLE, "")
            return node

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[PluginA(), PluginB()],
    )
    nodes, _ = loader.load(str(repo))

    foo = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Foo")
    assert foo.metadata[NK.ORM_TABLE] == "test_chain"
    assert foo.metadata[NK.ORM_FRAMEWORK] == "test_chain"  # B saw A's enrichment


def test_no_plugins_backward_compat(fs_result):
    """Loader works normally without plugins (backward compat)."""
    nodes, edges = fs_result
    assert len(nodes) > 0
    assert len(edges) > 0
    # No plugin metadata present
    for node in nodes:
        assert NK.ORM_TABLE not in node.metadata


# ── SQLAlchemy plugin tests ───────────────────────────────────────────


def test_sqlalchemy_plugin_detects_tablename(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "models.py").write_text(
        'class User:\n'
        '    __tablename__ = "users"\n'
        '    id: int\n'
    )

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))

    user = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "User")
    assert user.metadata[NK.ORM_TABLE] == "users"
    assert user.metadata[NK.ORM_FRAMEWORK] == "sqlalchemy"


def test_sqlalchemy_plugin_ignores_non_orm_classes(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "services.py").write_text(
        'class UserService:\n'
        '    def get_user(self):\n'
        '        pass\n'
    )

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))

    svc = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "UserService")
    assert NK.ORM_TABLE not in svc.metadata


def test_sqlalchemy_plugin_ignores_javascript(tmp_path):
    """SQLAlchemy plugin only fires for Python."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "model.js").write_text(
        'class User {\n'
        '    // __tablename__ = "users"  -- not Python!\n'
        '}\n'
    )

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))

    user = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "User")
    assert NK.ORM_TABLE not in user.metadata


def test_sqlalchemy_plugin_single_quotes(tmp_path):
    """Handles __tablename__ = 'value' with single quotes."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "m.py").write_text(
        "class Order:\n"
        "    __tablename__ = 'orders'\n"
    )

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))

    order = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Order")
    assert order.metadata[NK.ORM_TABLE] == "orders"


def test_sqlalchemy_plugin_multiple_models(tmp_path):
    """Multiple ORM models in one file are all detected."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "models.py").write_text(
        'class User:\n'
        '    __tablename__ = "users"\n'
        '\n'
        'class Order:\n'
        '    __tablename__ = "orders"\n'
        '\n'
        'class NotAModel:\n'
        '    pass\n'
    )

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))

    orm_nodes = [n for n in nodes if NK.ORM_TABLE in n.metadata]
    assert len(orm_nodes) == 2
    tables = {n.metadata[NK.ORM_TABLE] for n in orm_nodes}
    assert tables == {"users", "orders"}


# ── Orchestrator tests ────────────────────────────────────────────────


def _make_orm_repo(tmp_path):
    """Create a repo with ORM models + an API file that uses them."""
    repo = tmp_path / "code"
    repo.mkdir()
    (repo / "models.py").write_text(
        'class User:\n'
        '    __tablename__ = "users"\n'
        '    id: int\n'
        '    email: str\n'
        '\n'
        'class Order:\n'
        '    __tablename__ = "orders"\n'
        '    id: int\n'
    )
    (repo / "api.py").write_text(
        'from models import User\n'
        '\n'
        'def create_user():\n'
        '    user = User(email="test@example.com")\n'
        '    return user\n'
        '\n'
        'def list_orders():\n'
        '    orders = Order.query.all()\n'
        '    return orders\n'
        '\n'
        'def health_check():\n'
        '    return "ok"\n'
    )
    return repo


def _make_data_nodes():
    """Create fake database nodes mimicking OnPremPostgresLoader output."""
    from src.graph.loaders._helpers import make_edge

    db_urn = URN("urn:onprem:postgres:localhost:5432:mydb")
    schema_urn = URN("urn:onprem:postgres:localhost:5432:mydb/public")
    users_urn = URN("urn:onprem:postgres:localhost:5432:mydb/public/users")
    orders_urn = URN("urn:onprem:postgres:localhost:5432:mydb/public/orders")

    nodes = [
        Node(organization_id=ORG_ID, urn=db_urn, metadata=NodeMetadata({NK.DATABASE_NAME: "mydb"})),
        Node(organization_id=ORG_ID, urn=schema_urn, parent_urn=db_urn, metadata=NodeMetadata({NK.SCHEMA_NAME: "public"})),
        Node(organization_id=ORG_ID, urn=users_urn, parent_urn=schema_urn, metadata=NodeMetadata({NK.TABLE_NAME: "users"})),
        Node(organization_id=ORG_ID, urn=orders_urn, parent_urn=schema_urn, metadata=NodeMetadata({NK.TABLE_NAME: "orders"})),
    ]
    edges = [
        make_edge(ORG_ID, db_urn, schema_urn, RelationType.CONTAINS),
        make_edge(ORG_ID, schema_urn, users_urn, RelationType.CONTAINS),
        make_edge(ORG_ID, schema_urn, orders_urn, RelationType.CONTAINS),
    ]
    return nodes, edges


def test_stitch_orm_class_to_table(tmp_path):
    """ORM classes with orm_table link to matching table nodes."""
    repo = _make_orm_repo(tmp_path)
    data_nodes, data_edges = _make_data_nodes()

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    code_nodes, code_edges = loader.load(str(repo))

    all_nodes, all_edges = stitch_code_to_data(
        ORG_ID, data_nodes, data_edges, code_nodes, code_edges,
        code_base_paths=[str(repo)],
    )

    c2d = [e for e in all_edges if e.relation_type == RelationType.CODE_TO_DATA]
    orm_edges = [e for e in c2d if e.metadata.get(EK.DETECTION_METHOD) == "orm_tablename"]

    # User → users, Order → orders
    assert len(orm_edges) == 2
    targets = {e.metadata[EK.TABLE_NAME] for e in orm_edges}
    assert targets == {"users", "orders"}
    assert all(e.metadata[EK.CONFIDENCE] == 1.0 for e in orm_edges)


def test_stitch_function_to_table(tmp_path):
    """Functions referencing ORM class names get CODE_TO_DATA edges."""
    repo = _make_orm_repo(tmp_path)
    data_nodes, data_edges = _make_data_nodes()

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    code_nodes, code_edges = loader.load(str(repo))

    all_nodes, all_edges = stitch_code_to_data(
        ORG_ID, data_nodes, data_edges, code_nodes, code_edges,
        code_base_paths=[str(repo)],
    )

    c2d = [e for e in all_edges if e.relation_type == RelationType.CODE_TO_DATA]
    func_edges = [e for e in c2d if e.metadata.get(EK.DETECTION_METHOD) == "orm_reference"]

    # create_user references User → links to users table
    # list_orders references Order → links to orders table
    # health_check references neither → no edge
    func_to_table = {
        str(e.from_urn).rsplit("/", 1)[-1]: e.metadata[EK.TABLE_NAME]
        for e in func_edges
    }
    assert "create_user" in func_to_table
    assert func_to_table["create_user"] == "users"
    assert "list_orders" in func_to_table
    assert func_to_table["list_orders"] == "orders"
    assert "health_check" not in func_to_table


def test_stitch_no_data_nodes(tmp_path):
    """No crash when data graph has no table nodes."""
    repo = _make_orm_repo(tmp_path)
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    code_nodes, code_edges = loader.load(str(repo))

    all_nodes, all_edges = stitch_code_to_data(
        ORG_ID, [], [], code_nodes, code_edges,
        code_base_paths=[str(repo)],
    )

    c2d = [e for e in all_edges if e.relation_type == RelationType.CODE_TO_DATA]
    assert len(c2d) == 0


def test_stitch_no_orm_models(tmp_path):
    """No crash when code graph has no ORM models."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "plain.py").write_text('def hello():\n    pass\n')

    data_nodes, data_edges = _make_data_nodes()
    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    code_nodes, code_edges = loader.load(str(repo))

    all_nodes, all_edges = stitch_code_to_data(
        ORG_ID, data_nodes, data_edges, code_nodes, code_edges,
        code_base_paths=[str(repo)],
    )

    c2d = [e for e in all_edges if e.relation_type == RelationType.CODE_TO_DATA]
    assert len(c2d) == 0


def test_stitch_edge_metadata(tmp_path):
    """CODE_TO_DATA edges have proper metadata."""
    repo = _make_orm_repo(tmp_path)
    data_nodes, data_edges = _make_data_nodes()

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    code_nodes, code_edges = loader.load(str(repo))

    _, all_edges = stitch_code_to_data(
        ORG_ID, data_nodes, data_edges, code_nodes, code_edges,
        code_base_paths=[str(repo)],
    )

    c2d = [e for e in all_edges if e.relation_type == RelationType.CODE_TO_DATA]
    for edge in c2d:
        assert EK.DETECTION_METHOD in edge.metadata
        assert EK.CONFIDENCE in edge.metadata
        assert EK.TABLE_NAME in edge.metadata
        assert edge.organization_id == ORG_ID


# ── Boto3 S3 plugin tests ─────────────────────────────────────────────


def _make_s3_loader(*plugins):
    return FileSystemCodebaseLoader(
        organization_id=ORG_ID,
        plugins=list(plugins) if plugins else [Boto3S3Plugin()],
    )


def test_boto3_s3_class_with_client(tmp_path):
    """Class containing boto3.client('s3') gets AWS_S3_CLIENT tag."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "storage.py").write_text(
        'class Storage:\n'
        '    def __init__(self):\n'
        '        self.client = boto3.client("s3")\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    cls = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Storage")
    assert cls.metadata[NK.AWS_S3_CLIENT] is True


def test_boto3_s3_class_with_resource(tmp_path):
    """Class containing boto3.resource('s3') gets AWS_S3_CLIENT tag."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "storage.py").write_text(
        "class Storage:\n"
        "    def __init__(self):\n"
        "        self.s3 = boto3.resource('s3')\n"
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    cls = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Storage")
    assert cls.metadata[NK.AWS_S3_CLIENT] is True


def test_boto3_s3_class_with_session_client(tmp_path):
    """Class containing session.client('s3') gets AWS_S3_CLIENT tag."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "storage.py").write_text(
        'class Storage:\n'
        '    def __init__(self, session):\n'
        '        self.client = session.client("s3")\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    cls = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Storage")
    assert cls.metadata[NK.AWS_S3_CLIENT] is True


def test_boto3_s3_function_put_object(tmp_path):
    """Function with .put_object( gets write operation tag."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "upload.py").write_text(
        'def upload(client, data):\n'
        '    client.put_object(Bucket="b", Key="k", Body=data)\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "upload")
    assert fn.metadata[NK.AWS_S3_OPERATIONS] == "put_object"
    assert fn.metadata[NK.AWS_S3_OPERATION_TYPE] == "write"


def test_boto3_s3_function_get_object(tmp_path):
    """Function with .get_object( gets read operation tag."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "download.py").write_text(
        'def download(client):\n'
        '    return client.get_object(Bucket="b", Key="k")\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "download")
    assert fn.metadata[NK.AWS_S3_OPERATIONS] == "get_object"
    assert fn.metadata[NK.AWS_S3_OPERATION_TYPE] == "read"


def test_boto3_s3_function_delete_object(tmp_path):
    """Function with .delete_object( gets delete operation tag."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "cleanup.py").write_text(
        'def cleanup(client):\n'
        '    client.delete_object(Bucket="b", Key="k")\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "cleanup")
    assert fn.metadata[NK.AWS_S3_OPERATIONS] == "delete_object"
    assert fn.metadata[NK.AWS_S3_OPERATION_TYPE] == "delete"


def test_boto3_s3_function_mixed_ops(tmp_path):
    """Function with get + put gets both operations sorted."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "sync.py").write_text(
        'def sync(client):\n'
        '    data = client.get_object(Bucket="b", Key="k")\n'
        '    client.put_object(Bucket="b2", Key="k2", Body=data)\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "sync")
    assert fn.metadata[NK.AWS_S3_OPERATIONS] == "get_object,put_object"
    assert fn.metadata[NK.AWS_S3_OPERATION_TYPE] == "read,write"


def test_boto3_s3_function_paginator(tmp_path):
    """Function with get_paginator('list_objects_v2') gets read tag."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "lister.py").write_text(
        'def list_keys(client):\n'
        '    paginator = client.get_paginator("list_objects_v2")\n'
        '    for page in paginator.paginate(Bucket="b"):\n'
        '        yield page\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "list_keys")
    assert "list_objects_v2" in fn.metadata[NK.AWS_S3_OPERATIONS]
    assert fn.metadata[NK.AWS_S3_OPERATION_TYPE] == "read"


def test_boto3_s3_function_upload_download(tmp_path):
    """Function with upload_file + download_file gets read,write type."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "transfer.py").write_text(
        'def transfer(client):\n'
        '    client.upload_file("/tmp/a", "bucket", "key")\n'
        '    client.download_file("bucket", "key", "/tmp/b")\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "transfer")
    assert fn.metadata[NK.AWS_S3_OPERATIONS] == "download_file,upload_file"
    assert fn.metadata[NK.AWS_S3_OPERATION_TYPE] == "read,write"


def test_boto3_s3_function_client_and_ops(tmp_path):
    """Function creating client AND calling ops gets both tags."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "inline.py").write_text(
        'def store():\n'
        '    client = boto3.client("s3")\n'
        '    client.put_object(Bucket="b", Key="k", Body="data")\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "store")
    assert fn.metadata[NK.AWS_S3_CLIENT] is True
    assert fn.metadata[NK.AWS_S3_OPERATIONS] == "put_object"
    assert fn.metadata[NK.AWS_S3_OPERATION_TYPE] == "write"


def test_boto3_s3_class_methods_tagged_independently(tmp_path):
    """Each method in a class is tagged independently."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "multi.py").write_text(
        'class Store:\n'
        '    def __init__(self):\n'
        '        self.client = boto3.client("s3")\n'
        '\n'
        '    def save(self):\n'
        '        self.client.put_object(Bucket="b", Key="k", Body="x")\n'
        '\n'
        '    def process(self):\n'
        '        return 42\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))

    init = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "__init__")
    assert init.metadata[NK.AWS_S3_CLIENT] is True
    assert NK.AWS_S3_OPERATIONS not in init.metadata

    save = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "save")
    assert save.metadata[NK.AWS_S3_OPERATIONS] == "put_object"

    process = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "process")
    assert NK.AWS_S3_CLIENT not in process.metadata
    assert NK.AWS_S3_OPERATIONS not in process.metadata


def test_boto3_s3_ignores_non_s3_service(tmp_path):
    """boto3.client('sqs') should not trigger S3 tagging."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "queue.py").write_text(
        'class QueueClient:\n'
        '    def __init__(self):\n'
        '        self.client = boto3.client("sqs")\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    cls = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "QueueClient")
    assert NK.AWS_S3_CLIENT not in cls.metadata


def test_boto3_s3_ignores_javascript(tmp_path):
    """S3-like patterns in JavaScript should not be tagged."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "storage.js").write_text(
        'class Storage {\n'
        '    constructor() {\n'
        '        this.client = new S3Client({});\n'
        '        // boto3.client("s3") lookalike\n'
        '    }\n'
        '}\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    cls = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Storage")
    assert NK.AWS_S3_CLIENT not in cls.metadata


def test_boto3_s3_no_tags_on_plain_class(tmp_path):
    """A class with no S3 patterns gets no S3 tags."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "plain.py").write_text(
        'class PlainService:\n'
        '    def run(self):\n'
        '        return True\n'
    )

    nodes, _ = _make_s3_loader().load(str(repo))
    cls = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "PlainService")
    assert NK.AWS_S3_CLIENT not in cls.metadata
    assert NK.AWS_S3_OPERATIONS not in cls.metadata
