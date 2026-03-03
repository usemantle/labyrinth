"""Unit tests for the Requests codebase plugin."""

import uuid

from src.graph.graph_models import NodeMetadataKey
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from src.graph.loaders.codebase.plugins import RequestsPlugin

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey


def _load(tmp_path, source):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "client.py").write_text(source)
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID,
        plugins=[RequestsPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    return nodes


def _find_func(nodes, name):
    return next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == name)


def test_requests_get(tmp_path):
    nodes = _load(tmp_path, (
        "def fetch():\n"
        "    return requests.get('https://api.example.com/data')\n"
    ))
    fn = _find_func(nodes, "fetch")
    assert fn.metadata[NK.IO_DIRECTION] == "egress"
    assert fn.metadata[NK.IO_TYPE] == "network"


def test_requests_post(tmp_path):
    nodes = _load(tmp_path, (
        "def send():\n"
        "    return requests.post('https://api.example.com/data', json={})\n"
    ))
    fn = _find_func(nodes, "send")
    assert fn.metadata[NK.IO_DIRECTION] == "egress"
    assert fn.metadata[NK.IO_TYPE] == "network"


def test_requests_put(tmp_path):
    nodes = _load(tmp_path, (
        "def update():\n"
        "    return requests.put('https://api.example.com/item/1', json={})\n"
    ))
    fn = _find_func(nodes, "update")
    assert fn.metadata[NK.IO_DIRECTION] == "egress"
    assert fn.metadata[NK.IO_TYPE] == "network"


def test_requests_delete(tmp_path):
    nodes = _load(tmp_path, (
        "def remove():\n"
        "    return requests.delete('https://api.example.com/item/1')\n"
    ))
    fn = _find_func(nodes, "remove")
    assert fn.metadata[NK.IO_DIRECTION] == "egress"
    assert fn.metadata[NK.IO_TYPE] == "network"


def test_requests_session(tmp_path):
    nodes = _load(tmp_path, (
        "def create_session():\n"
        "    s = requests.Session()\n"
        "    return s\n"
    ))
    fn = _find_func(nodes, "create_session")
    assert fn.metadata[NK.IO_DIRECTION] == "egress"
    assert fn.metadata[NK.IO_TYPE] == "network"


def test_no_requests_call(tmp_path):
    nodes = _load(tmp_path, (
        "def compute():\n"
        "    return 1 + 2\n"
    ))
    fn = _find_func(nodes, "compute")
    assert NK.IO_DIRECTION not in fn.metadata
    assert NK.IO_TYPE not in fn.metadata


def test_ignores_javascript(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "client.js").write_text(
        "function fetch() {\n"
        "    // requests.get('https://api.example.com')\n"
        "    return null;\n"
        "}\n"
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID,
        plugins=[RequestsPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    fn = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "fetch")
    assert NK.IO_DIRECTION not in fn.metadata
