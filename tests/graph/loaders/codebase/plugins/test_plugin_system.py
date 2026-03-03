"""
Unit tests for the codebase plugin system (CodebasePlugin base class).

Verifies that plugins receive class/function nodes, run in order,
and that the loader works without plugins (backward compat).
"""

import uuid

import pytest

from src.graph.graph_models import (
    NodeMetadataKey,
)
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from src.graph.loaders.codebase.plugins import CodebasePlugin

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

NK = NodeMetadataKey


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


def test_no_plugins_backward_compat(tmp_path):
    """Loader works normally without plugins (backward compat)."""
    from pathlib import Path

    repo = tmp_path / "my-repo"
    repo.mkdir()
    (repo / "app.py").write_text(
        'class UserService:\n'
        '    def get_user(self, user_id):\n'
        '        return {"id": user_id}\n'
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, edges = loader.load(str(repo))
    assert len(nodes) > 0
    assert len(edges) > 0
    # No plugin metadata present
    for node in nodes:
        assert NK.ORM_TABLE not in node.metadata
