"""Unit tests for the UV lockfile plugin."""

import json
import os
import uuid
from unittest.mock import patch

from mcp.server.fastmcp import FastMCP

from src.graph.graph_models import NodeMetadataKey
from src.graph.loaders.codebase.cve.osv_client import OsvResult
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from src.graph.loaders.codebase.plugins import UvPlugin
from src.mcp.graph_store import GraphStore
from src.mcp.tools.security import register

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey

LOCK_CONTENT = """\
version = 1

[[package]]
name = "requests"
version = "2.31.0"
source = { registry = "https://pypi.org/simple" }

[[package]]
name = "flask"
version = "2.0.0"
source = { registry = "https://pypi.org/simple" }
"""

LOCK_WITH_DEPS = """\
version = 1

[[package]]
name = "my-app"
version = "0.1.0"
source = { editable = "." }
dependencies = [
    { name = "python-jose" },
    { name = "requests" },
]

[[package]]
name = "python-jose"
version = "3.3.0"
source = { registry = "https://pypi.org/simple" }
dependencies = [
    { name = "cryptography" },
    { name = "ecdsa" },
]

[[package]]
name = "cryptography"
version = "46.0.3"
source = { registry = "https://pypi.org/simple" }

[[package]]
name = "ecdsa"
version = "0.19.1"
source = { registry = "https://pypi.org/simple" }

[[package]]
name = "requests"
version = "2.31.0"
source = { registry = "https://pypi.org/simple" }
dependencies = [
    { name = "urllib3" },
]

[[package]]
name = "urllib3"
version = "2.2.1"
source = { registry = "https://pypi.org/simple" }
"""


def _make_repo(tmp_path, lock_content=LOCK_CONTENT, add_py=True):
    repo = tmp_path / "repo"
    repo.mkdir()
    if lock_content is not None:
        (repo / "uv.lock").write_text(lock_content)
    if add_py:
        (repo / "main.py").write_text("def hello():\n    return 'hi'\n")
    return repo


def _load(repo):
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID,
        plugins=[UvPlugin()],
    )
    return loader.load(str(repo))


def _find_dep(nodes, name):
    for n in nodes:
        if n.metadata.get(NK.PACKAGE_NAME) == name:
            return n
    return None


@patch("src.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_package_node_creation(mock_osv, tmp_path):
    mock_osv.return_value = OsvResult()
    repo = _make_repo(tmp_path)
    nodes, _ = _load(repo)

    req = _find_dep(nodes, "requests")
    assert req is not None
    assert req.metadata[NK.PACKAGE_VERSION] == "2.31.0"
    assert req.metadata[NK.PACKAGE_ECOSYSTEM] == "PyPI"

    flask = _find_dep(nodes, "flask")
    assert flask is not None
    assert flask.metadata[NK.PACKAGE_VERSION] == "2.0.0"


@patch("src.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_contains_edges(mock_osv, tmp_path):
    mock_osv.return_value = OsvResult()
    repo = _make_repo(tmp_path)
    nodes, edges = _load(repo)

    req = _find_dep(nodes, "requests")
    contains_edges = [
        e for e in edges
        if e.edge_type == "contains"
        and e.to_urn == req.urn
    ]
    assert len(contains_edges) == 1


@patch("src.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_cve_on_vulnerable_package(mock_osv, tmp_path):
    def osv_side_effect(name, version, ecosystem):
        if name == "requests":
            return OsvResult(cve_ids=["CVE-2023-32681"])
        return OsvResult()

    mock_osv.side_effect = osv_side_effect
    repo = _make_repo(tmp_path)
    nodes, _ = _load(repo)

    req = _find_dep(nodes, "requests")
    assert req.metadata[NK.CVE_IDS] == "CVE-2023-32681"


@patch("src.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_no_cve_on_clean_package(mock_osv, tmp_path):
    mock_osv.return_value = OsvResult()
    repo = _make_repo(tmp_path)
    nodes, _ = _load(repo)

    flask = _find_dep(nodes, "flask")
    assert NK.CVE_IDS not in flask.metadata


@patch("src.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_missing_lock_file(mock_osv, tmp_path):
    repo = _make_repo(tmp_path, lock_content=None)
    nodes, edges = _load(repo)

    deps = [n for n in nodes if NK.PACKAGE_NAME in n.metadata]
    assert deps == []
    mock_osv.assert_not_called()


@patch("src.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_osv_error_resilience(mock_osv, tmp_path):
    mock_osv.return_value = OsvResult(error="timeout")
    repo = _make_repo(tmp_path)
    nodes, _ = _load(repo)

    req = _find_dep(nodes, "requests")
    assert req is not None
    assert NK.CVE_IDS not in req.metadata


@patch("src.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_multiple_cves_comma_separated(mock_osv, tmp_path):
    mock_osv.return_value = OsvResult(
        cve_ids=["CVE-2021-11111", "CVE-2022-22222"]
    )
    repo = _make_repo(tmp_path)
    nodes, _ = _load(repo)

    req = _find_dep(nodes, "requests")
    assert req.metadata[NK.CVE_IDS] == "CVE-2021-11111,CVE-2022-22222"


@patch("src.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_transitive_depends_on_edges(mock_osv, tmp_path):
    """Dependency nodes are linked via DEPENDS_ON based on uv.lock dependencies."""
    mock_osv.return_value = OsvResult()
    repo = _make_repo(tmp_path, lock_content=LOCK_WITH_DEPS)
    nodes, edges = _load(repo)

    jose = _find_dep(nodes, "python-jose")
    crypto = _find_dep(nodes, "cryptography")
    ecdsa = _find_dep(nodes, "ecdsa")
    assert jose and crypto and ecdsa

    dep_edges = [e for e in edges if e.edge_type == "depends_on"]
    # python-jose → cryptography
    assert any(e.from_urn == jose.urn and e.to_urn == crypto.urn for e in dep_edges)
    # python-jose → ecdsa
    assert any(e.from_urn == jose.urn and e.to_urn == ecdsa.urn for e in dep_edges)


@patch("src.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_transitive_chain_depth(mock_osv, tmp_path):
    """my-app → requests → urllib3 forms a transitive chain."""
    mock_osv.return_value = OsvResult()
    repo = _make_repo(tmp_path, lock_content=LOCK_WITH_DEPS)
    nodes, edges = _load(repo)

    app = _find_dep(nodes, "my-app")
    requests = _find_dep(nodes, "requests")
    urllib3 = _find_dep(nodes, "urllib3")
    assert app and requests and urllib3

    dep_edges = [e for e in edges if e.edge_type == "depends_on"]
    # my-app → requests
    assert any(e.from_urn == app.urn and e.to_urn == requests.urn for e in dep_edges)
    # requests → urllib3
    assert any(e.from_urn == requests.urn and e.to_urn == urllib3.urn for e in dep_edges)


@patch("src.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_no_transitive_edges_without_dependencies(mock_osv, tmp_path):
    """Packages without a dependencies field produce no DEPENDS_ON edges."""
    mock_osv.return_value = OsvResult()
    repo = _make_repo(tmp_path, lock_content=LOCK_CONTENT)
    nodes, edges = _load(repo)

    dep_edges = [e for e in edges if e.edge_type == "depends_on"]
    assert dep_edges == []


@patch("src.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_transitive_cve_reachable_via_blast_radius(mock_osv, tmp_path):
    """blast_radius from a file importing python-jose reaches cryptography's CVE."""
    def osv_side_effect(name, version, ecosystem):
        if name == "cryptography":
            return OsvResult(cve_ids=["CVE-2026-26007"])
        return OsvResult()

    mock_osv.side_effect = osv_side_effect
    repo = _make_repo(tmp_path, lock_content=LOCK_WITH_DEPS)
    nodes, edges = _load(repo)

    # Serialize to JSON and load into GraphStore
    graph_path = os.path.join(str(tmp_path), "graph.json")
    graph_data = {
        "generated_at": "2024-01-01T00:00:00Z",
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": [
            {
                "urn": str(n.urn),
                "organization_id": str(n.organization_id),
                "parent_urn": str(n.parent_urn) if n.parent_urn else None,
                "node_type": n.metadata.get(NK.PACKAGE_NAME) and "dependency"
                    or n.metadata.get(NK.FILE_PATH) and "file"
                    or n.metadata.get(NK.FUNCTION_NAME) and "function"
                    or "codebase",
                "metadata": dict(n.metadata.items()),
            }
            for n in nodes
        ],
        "edges": [
            {
                "uuid": str(e.uuid),
                "organization_id": str(e.organization_id),
                "from_urn": str(e.from_urn),
                "to_urn": str(e.to_urn),
                "edge_type": e.edge_type,
                "metadata": dict(e.metadata.items()),
            }
            for e in edges
        ],
    }
    with open(graph_path, "w") as f:
        json.dump(graph_data, f)

    store = GraphStore(graph_path)
    mcp = FastMCP("test")
    register(mcp, store)
    blast_fn = mcp._tool_manager._tools["blast_radius"].fn

    # Find the python-jose dep node URN
    jose = _find_dep(nodes, "python-jose")
    result = blast_fn(urn=str(jose.urn), max_depth=5)

    assert "cryptography" in result
    assert "CVE-2026-26007" in result
    assert "CVE-affected dependencies: 1" in result
