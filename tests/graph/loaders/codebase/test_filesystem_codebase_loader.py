"""
Unit tests for FileSystemCodebaseLoader.

All filesystem access is on real tmp directories — no mocking required.
"""

import uuid
from pathlib import Path

import pytest

from src.graph.graph_models import (
    EdgeMetadataKey,
    NodeMetadataKey,
    RelationType,
)
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader

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
