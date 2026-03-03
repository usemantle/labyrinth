"""Unit tests for the Flask codebase plugin."""

import uuid

from src.graph.graph_models import NodeMetadataKey
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from src.graph.loaders.codebase.plugins import FlaskPlugin

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey


def _load(tmp_path, source):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "app.py").write_text(source)
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID,
        plugins=[FlaskPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    return nodes


def _find_func(nodes, name):
    return next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == name)


def test_app_route_get(tmp_path):
    nodes = _load(tmp_path, (
        "@app.route('/users')\n"
        "def list_users():\n"
        "    return []\n"
    ))
    fn = _find_func(nodes, "list_users")
    assert fn.metadata[NK.IO_DIRECTION] == "ingress"
    assert fn.metadata[NK.IO_TYPE] == "network"
    assert fn.metadata[NK.ROUTE_PATH] == "/users"
    assert fn.metadata[NK.API_FRAMEWORK] == "flask"


def test_route_with_methods(tmp_path):
    nodes = _load(tmp_path, (
        '@app.route("/users", methods=["POST"])\n'
        "def create_user():\n"
        "    return {}\n"
    ))
    fn = _find_func(nodes, "create_user")
    assert fn.metadata[NK.IO_DIRECTION] == "ingress"
    assert fn.metadata[NK.IO_TYPE] == "network"
    assert fn.metadata[NK.HTTP_METHOD] == "POST"


def test_blueprint_route(tmp_path):
    nodes = _load(tmp_path, (
        "@bp.route('/items/<int:item_id>')\n"
        "def get_item(item_id):\n"
        "    return {}\n"
    ))
    fn = _find_func(nodes, "get_item")
    assert fn.metadata[NK.IO_DIRECTION] == "ingress"
    assert fn.metadata[NK.IO_TYPE] == "network"
    assert fn.metadata[NK.ROUTE_PATH] == "/items/<int:item_id>"
    assert fn.metadata[NK.API_FRAMEWORK] == "flask"


def test_no_flask_route(tmp_path):
    nodes = _load(tmp_path, (
        "def compute():\n"
        "    return 42\n"
    ))
    fn = _find_func(nodes, "compute")
    assert NK.IO_DIRECTION not in fn.metadata
    assert NK.ROUTE_PATH not in fn.metadata


def test_ignores_javascript(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "app.js").write_text(
        "// @app.route('/users')\n"
        "function list_users() {\n"
        "    return [];\n"
        "}\n"
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID,
        plugins=[FlaskPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "list_users")
    assert NK.IO_DIRECTION not in fn.metadata
