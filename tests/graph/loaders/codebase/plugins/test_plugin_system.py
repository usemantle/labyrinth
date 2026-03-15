"""
Unit tests for the codebase plugin system (CodebasePlugin base class).

Verifies that plugins enrich nodes via post_process, run in order,
and that the loader works without plugins (backward compat).
"""

import uuid

from src.graph.graph_models import (
    NodeMetadataKey,
)
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from src.graph.loaders.codebase.plugins import CodebasePlugin

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

NK = NodeMetadataKey


class _TrackingPlugin(CodebasePlugin):
    """Test plugin that records what it was called with during post_process."""

    def __init__(self):
        self.class_names = []
        self.function_names = []

    def supported_languages(self):
        return {'python'}

    def post_process(self, nodes, edges, context):
        for node in nodes:
            if NK.CLASS_NAME in node.metadata:
                self.class_names.append(node.metadata[NK.CLASS_NAME])
                node.metadata[NK.ORM_TABLE] = "__tracked__"
            elif NK.FUNCTION_NAME in node.metadata:
                self.function_names.append(node.metadata[NK.FUNCTION_NAME])
                node.metadata[NK.ORM_TABLE] = "__tracked__"
        return nodes, edges


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

    assert len(plugin.class_names) == 1
    assert plugin.class_names[0] == "User"

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

    assert "hello" in plugin.function_names


def test_plugin_skips_non_python_for_language_check(tmp_path):
    """Python-only plugin is not called for JS files."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "app.js").write_text(
        'class App {}\n'
        'function main() {}\n'
    )

    plugin = _TrackingPlugin()
    loader = FileSystemCodebaseLoader(organization_id=ORG_ID, plugins=[plugin])
    loader.load(str(repo))

    assert len(plugin.class_names) == 0
    assert len(plugin.function_names) == 0


def test_multiple_plugins_chain(tmp_path):
    """Multiple plugins are called in order and each sees the enriched node."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "m.py").write_text('class Foo:\n    pass\n')

    class PluginA(CodebasePlugin):
        def supported_languages(self):
            return {"python"}

        def post_process(self, nodes, edges, context):
            for node in nodes:
                if NK.CLASS_NAME in node.metadata:
                    node.metadata[NK.ORM_TABLE] = "test_chain"
            return nodes, edges

    class PluginB(CodebasePlugin):
        def supported_languages(self):
            return {"python"}

        def post_process(self, nodes, edges, context):
            for node in nodes:
                if NK.CLASS_NAME in node.metadata:
                    node.metadata[NK.ORM_FRAMEWORK] = node.metadata.get(NK.ORM_TABLE, "")
            return nodes, edges

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[PluginA(), PluginB()],
    )
    nodes, _ = loader.load(str(repo))

    foo = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Foo")
    assert foo.metadata[NK.ORM_TABLE] == "test_chain"
    assert foo.metadata[NK.ORM_FRAMEWORK] == "test_chain"  # B saw A's enrichment


def test_no_plugins_backward_compat(tmp_path):
    """Loader works normally without plugins (backward compat)."""

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
