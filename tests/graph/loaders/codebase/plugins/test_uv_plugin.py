"""Unit tests for the UV lockfile plugin."""

import uuid
from unittest.mock import patch

from src.graph.graph_models import NodeMetadataKey, RelationType
from src.graph.loaders.codebase.cve.osv_client import OsvResult
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from src.graph.loaders.codebase.plugins import UvPlugin

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
        if e.relation_type == RelationType.CONTAINS
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
