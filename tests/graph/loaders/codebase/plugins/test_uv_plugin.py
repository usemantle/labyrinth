"""Unit tests for the UV lockfile plugin."""

import uuid
from unittest.mock import patch

from labyrinth.graph.graph_models import NodeMetadataKey, NodeType
from labyrinth.graph.loaders.codebase.cve.osv_client import OsvResult
from labyrinth.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from labyrinth.graph.loaders.codebase.plugins import UvPlugin

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


def _find_manifest(nodes):
    for n in nodes:
        if n.node_type == NodeType.PACKAGE_MANIFEST:
            return n
    return None


@patch("labyrinth.graph.loaders.codebase.plugins.uv_plugin.query_osv")
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


@patch("labyrinth.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_manifest_node_created(mock_osv, tmp_path):
    """A package_manifest node is created to represent the lockfile."""
    mock_osv.return_value = OsvResult()
    repo = _make_repo(tmp_path)
    nodes, edges = _load(repo)

    manifest = _find_manifest(nodes)
    assert manifest is not None
    assert manifest.metadata[NK.PACKAGE_MANAGER] == "uv"
    assert manifest.metadata[NK.MANIFEST_FILE] == "uv.lock"

    # Codebase contains the manifest
    contains_to_manifest = [
        e for e in edges
        if e.edge_type == "contains" and e.to_urn == manifest.urn
    ]
    assert len(contains_to_manifest) == 1


@patch("labyrinth.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_manifest_depends_on_dependencies(mock_osv, tmp_path):
    """The manifest node has depends_on edges to each dependency."""
    mock_osv.return_value = OsvResult()
    repo = _make_repo(tmp_path)
    nodes, edges = _load(repo)

    manifest = _find_manifest(nodes)
    req = _find_dep(nodes, "requests")

    depends_on_edges = [
        e for e in edges
        if e.edge_type == "depends_on"
        and e.from_urn == manifest.urn
        and e.to_urn == req.urn
    ]
    assert len(depends_on_edges) == 1


@patch("labyrinth.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_no_contains_edge_to_dependency(mock_osv, tmp_path):
    """Dependencies should NOT have contains edges from the codebase."""
    mock_osv.return_value = OsvResult()
    repo = _make_repo(tmp_path)
    nodes, edges = _load(repo)

    req = _find_dep(nodes, "requests")
    contains_to_dep = [
        e for e in edges
        if e.edge_type == "contains" and e.to_urn == req.urn
    ]
    assert len(contains_to_dep) == 0


@patch("labyrinth.graph.loaders.codebase.plugins.uv_plugin.query_osv")
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


@patch("labyrinth.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_no_cve_on_clean_package(mock_osv, tmp_path):
    mock_osv.return_value = OsvResult()
    repo = _make_repo(tmp_path)
    nodes, _ = _load(repo)

    flask = _find_dep(nodes, "flask")
    assert NK.CVE_IDS not in flask.metadata


@patch("labyrinth.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_missing_lock_file(mock_osv, tmp_path):
    repo = _make_repo(tmp_path, lock_content=None)
    nodes, edges = _load(repo)

    deps = [n for n in nodes if NK.PACKAGE_NAME in n.metadata]
    assert deps == []
    mock_osv.assert_not_called()


@patch("labyrinth.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_osv_error_resilience(mock_osv, tmp_path):
    mock_osv.return_value = OsvResult(error="timeout")
    repo = _make_repo(tmp_path)
    nodes, _ = _load(repo)

    req = _find_dep(nodes, "requests")
    assert req is not None
    assert NK.CVE_IDS not in req.metadata


@patch("labyrinth.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_multiple_cves_comma_separated(mock_osv, tmp_path):
    mock_osv.return_value = OsvResult(
        cve_ids=["CVE-2021-11111", "CVE-2022-22222"]
    )
    repo = _make_repo(tmp_path)
    nodes, _ = _load(repo)

    req = _find_dep(nodes, "requests")
    assert req.metadata[NK.CVE_IDS] == "CVE-2021-11111,CVE-2022-22222"


@patch("labyrinth.graph.loaders.codebase.plugins.uv_plugin.query_osv")
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


@patch("labyrinth.graph.loaders.codebase.plugins.uv_plugin.query_osv")
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


@patch("labyrinth.graph.loaders.codebase.plugins.uv_plugin.query_osv")
def test_no_transitive_edges_without_dependencies(mock_osv, tmp_path):
    """Packages without a dependencies field produce no transitive DEPENDS_ON edges."""
    mock_osv.return_value = OsvResult()
    repo = _make_repo(tmp_path, lock_content=LOCK_CONTENT)
    nodes, edges = _load(repo)

    # Only manifest → dep edges, no dep → dep edges
    manifest = _find_manifest(nodes)
    dep_edges = [e for e in edges if e.edge_type == "depends_on"]
    # All depends_on edges should be from the manifest
    assert all(e.from_urn == manifest.urn for e in dep_edges)


