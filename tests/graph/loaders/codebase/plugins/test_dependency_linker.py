"""Tests for the dependency linker codebase plugin."""

import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

from src.graph.graph_models import (
    Edge,
    Node,
    NodeMetadata,
    NodeMetadataKey,
    RelationType,
    URN,
)
from src.graph.loaders._helpers import make_edge
from src.graph.loaders.codebase.codebase_loader import PostProcessContext
from src.graph.loaders.codebase.plugins.dependency_linker import (
    DependencyLinkerPlugin,
    _normalize_package_to_import,
)

NK = NodeMetadataKey
ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


def _build_urn(*segments: str) -> URN:
    path = "/".join(segments)
    return URN(f"urn:github:repo:::myapp/{path}" if path else "urn:github:repo:::myapp")


def _make_dep_node(name: str, version: str = "1.0.0", cve_ids: str = "") -> Node:
    meta = NodeMetadata({
        NK.PACKAGE_NAME: name,
        NK.PACKAGE_VERSION: version,
        NK.PACKAGE_ECOSYSTEM: "PyPI",
    })
    if cve_ids:
        meta[NK.CVE_IDS] = cve_ids
    return Node(
        organization_id=ORG_ID,
        urn=_build_urn(f"dep/{name}"),
        parent_urn=_build_urn(""),
        metadata=meta,
    )


def _make_file_node(rel_path: str) -> Node:
    return Node(
        organization_id=ORG_ID,
        urn=_build_urn(rel_path),
        parent_urn=_build_urn(""),
        metadata=NodeMetadata({
            NK.FILE_PATH: rel_path,
            NK.LANGUAGE: "python",
        }),
    )


def _make_context(file_sources: dict[str, str]) -> PostProcessContext:
    return PostProcessContext(
        root_path=Path("/tmp/myapp"),
        root_name="myapp",
        organization_id=ORG_ID,
        file_sources=file_sources,
        file_languages={k: "python" for k in file_sources},
        build_urn=_build_urn,
    )


# ── _normalize_package_to_import tests ─────────────────────────────────


class TestNormalizePackageName:
    def test_normalize_known_packages(self):
        assert _normalize_package_to_import("requests") == "requests"
        assert _normalize_package_to_import("flask") == "flask"
        assert _normalize_package_to_import("sqlalchemy") == "sqlalchemy"

    def test_normalize_special_cases(self):
        assert _normalize_package_to_import("Pillow") == "PIL"
        assert _normalize_package_to_import("python-dateutil") == "dateutil"
        assert _normalize_package_to_import("beautifulsoup4") == "bs4"
        assert _normalize_package_to_import("scikit-learn") == "sklearn"

    def test_normalize_hyphenated(self):
        assert _normalize_package_to_import("my-package") == "my_package"


# ── DependencyLinkerPlugin tests ───────────────────────────────────────


class TestDependencyLinkerPlugin:
    def test_depends_on_edge_created(self):
        plugin = DependencyLinkerPlugin()
        dep_node = _make_dep_node("requests")
        file_node = _make_file_node("src/api.py")
        nodes = [file_node, dep_node]
        edges = []
        ctx = _make_context({"src/api.py": "import requests\n\ndef fetch():\n    pass\n"})

        new_nodes, new_edges = plugin.post_process(nodes, edges, ctx)
        depends_on = [e for e in new_edges if e.relation_type == RelationType.DEPENDS_ON]
        assert len(depends_on) == 1
        assert str(depends_on[0].from_urn) == str(file_node.urn)
        assert str(depends_on[0].to_urn) == str(dep_node.urn)

    def test_no_edge_for_untracked_import(self):
        plugin = DependencyLinkerPlugin()
        dep_node = _make_dep_node("requests")
        file_node = _make_file_node("src/api.py")
        nodes = [file_node, dep_node]
        ctx = _make_context({"src/api.py": "import os\nimport sys\n"})

        _, new_edges = plugin.post_process(nodes, [], ctx)
        depends_on = [e for e in new_edges if e.relation_type == RelationType.DEPENDS_ON]
        assert len(depends_on) == 0

    def test_multiple_files_same_dependency(self):
        plugin = DependencyLinkerPlugin()
        dep_node = _make_dep_node("requests")
        file1 = _make_file_node("src/api.py")
        file2 = _make_file_node("src/client.py")
        nodes = [file1, file2, dep_node]
        ctx = _make_context({
            "src/api.py": "import requests\n",
            "src/client.py": "import requests\n",
        })

        _, new_edges = plugin.post_process(nodes, [], ctx)
        depends_on = [e for e in new_edges if e.relation_type == RelationType.DEPENDS_ON]
        assert len(depends_on) == 2

    def test_no_dependency_nodes_no_crash(self):
        plugin = DependencyLinkerPlugin()
        file_node = _make_file_node("src/api.py")
        ctx = _make_context({"src/api.py": "import requests\n"})

        nodes, edges = plugin.post_process([file_node], [], ctx)
        assert len(edges) == 0

    def test_edge_metadata_has_import_name(self):
        plugin = DependencyLinkerPlugin()
        dep_node = _make_dep_node("requests")
        file_node = _make_file_node("src/api.py")
        nodes = [file_node, dep_node]
        ctx = _make_context({"src/api.py": "import requests\n"})

        _, new_edges = plugin.post_process(nodes, [], ctx)
        depends_on = [e for e in new_edges if e.relation_type == RelationType.DEPENDS_ON]
        assert depends_on[0].metadata["import_name"] == "requests"

    def test_from_import_detected(self):
        plugin = DependencyLinkerPlugin()
        dep_node = _make_dep_node("flask")
        file_node = _make_file_node("src/app.py")
        nodes = [file_node, dep_node]
        ctx = _make_context({"src/app.py": "from flask import Flask\n"})

        _, new_edges = plugin.post_process(nodes, [], ctx)
        depends_on = [e for e in new_edges if e.relation_type == RelationType.DEPENDS_ON]
        assert len(depends_on) == 1

    def test_special_package_name_mapping(self):
        plugin = DependencyLinkerPlugin()
        dep_node = _make_dep_node("Pillow")
        file_node = _make_file_node("src/images.py")
        nodes = [file_node, dep_node]
        ctx = _make_context({"src/images.py": "from PIL import Image\n"})

        _, new_edges = plugin.post_process(nodes, [], ctx)
        depends_on = [e for e in new_edges if e.relation_type == RelationType.DEPENDS_ON]
        assert len(depends_on) == 1
