"""Tests for the dependency linker plugins (ABC and Python implementation)."""

import uuid
from pathlib import Path

from src.graph.graph_models import (
    URN,
    Node,
    NodeMetadata,
    NodeMetadataKey,
)
from src.graph.loaders.codebase.codebase_loader import PostProcessContext
from src.graph.loaders.codebase.plugins.python_dependency_linker import (
    PythonDependencyLinkerPlugin,
    _build_import_map,
    _find_site_packages,
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


def _make_context(file_sources: dict[str, str], root_path: Path | None = None) -> PostProcessContext:
    return PostProcessContext(
        root_path=root_path or Path("/tmp/myapp"),
        root_name="myapp",
        organization_id=ORG_ID,
        file_sources=file_sources,
        file_languages=dict.fromkeys(file_sources, "python"),
        build_urn=_build_urn,
    )


# ── Import extraction tests ──────────────────────────────────────────


class TestExtractImports:
    def test_import_statement(self):
        plugin = PythonDependencyLinkerPlugin()
        imports = plugin.extract_imports("import requests\nimport os\n")
        assert "requests" in imports
        assert "os" in imports

    def test_from_import_statement(self):
        plugin = PythonDependencyLinkerPlugin()
        imports = plugin.extract_imports("from flask import Flask\n")
        assert "flask" in imports

    def test_mixed_imports(self):
        plugin = PythonDependencyLinkerPlugin()
        imports = plugin.extract_imports("import requests\nfrom flask import Flask\n")
        assert "requests" in imports
        assert "flask" in imports

    def test_case_insensitive(self):
        plugin = PythonDependencyLinkerPlugin()
        imports = plugin.extract_imports("import Requests\n")
        assert "requests" in imports


# ── Import name resolution tests ──────────────────────────────────────


class TestResolveImportNames:
    def test_fallback_normalization(self):
        """Without a venv, falls back to hyphen→underscore normalization."""
        plugin = PythonDependencyLinkerPlugin()
        ctx = _make_context({})
        assert plugin.resolve_import_names("my-package", ctx) == {"my_package"}

    def test_fallback_lowercase(self):
        plugin = PythonDependencyLinkerPlugin()
        ctx = _make_context({})
        assert plugin.resolve_import_names("MyPackage", ctx) == {"mypackage"}

    def test_reads_top_level_txt(self, tmp_path):
        """When a venv with top_level.txt exists, uses it."""
        # Create a fake venv with dist-info
        sp = tmp_path / ".venv" / "lib" / "python3.13" / "site-packages"
        dist = sp / "Pillow-12.0.0.dist-info"
        dist.mkdir(parents=True)
        (dist / "top_level.txt").write_text("PIL\n")

        plugin = PythonDependencyLinkerPlugin()
        ctx = _make_context({}, root_path=tmp_path)
        result = plugin.resolve_import_names("pillow", ctx)
        assert "pil" in result

    def test_reads_record_fallback(self, tmp_path):
        """Falls back to RECORD parsing when top_level.txt is missing."""
        sp = tmp_path / ".venv" / "lib" / "python3.13" / "site-packages"
        dist = sp / "cryptography-46.0.3.dist-info"
        dist.mkdir(parents=True)
        (dist / "RECORD").write_text(
            "cryptography/__init__.py,sha256=abc,123\n"
            "cryptography/fernet.py,sha256=def,456\n"
            "cryptography-46.0.3.dist-info/METADATA,sha256=ghi,789\n"
        )

        plugin = PythonDependencyLinkerPlugin()
        ctx = _make_context({}, root_path=tmp_path)
        result = plugin.resolve_import_names("cryptography", ctx)
        assert "cryptography" in result


# ── Site-packages discovery tests ─────────────────────────────────────


class TestFindSitePackages:
    def test_finds_venv(self, tmp_path):
        sp = tmp_path / ".venv" / "lib" / "python3.13" / "site-packages"
        sp.mkdir(parents=True)
        assert _find_site_packages(tmp_path) == sp

    def test_finds_venv_dir(self, tmp_path):
        sp = tmp_path / "venv" / "lib" / "python3.13" / "site-packages"
        sp.mkdir(parents=True)
        assert _find_site_packages(tmp_path) == sp

    def test_no_venv_returns_none(self, tmp_path):
        assert _find_site_packages(tmp_path) is None


# ── Import map building tests ─────────────────────────────────────────


class TestBuildImportMap:
    def test_top_level_txt(self, tmp_path):
        dist = tmp_path / "python_dateutil-2.9.0.dist-info"
        dist.mkdir()
        (dist / "top_level.txt").write_text("dateutil\n")

        result = _build_import_map(tmp_path)
        assert "python-dateutil" in result
        assert "dateutil" in result["python-dateutil"]

    def test_record_fallback(self, tmp_path):
        dist = tmp_path / "attrs-25.4.0.dist-info"
        dist.mkdir()
        (dist / "RECORD").write_text(
            "attr/__init__.py,sha256=abc,123\n"
            "attrs/__init__.py,sha256=def,456\n"
            "attrs-25.4.0.dist-info/METADATA,sha256=ghi,789\n"
        )

        result = _build_import_map(tmp_path)
        assert "attrs" in result
        assert "attr" in result["attrs"]
        assert "attrs" in result["attrs"]

    def test_multiple_packages(self, tmp_path):
        for name, tl in [("requests-2.31.0", "requests"), ("flask-2.0.0", "flask")]:
            dist = tmp_path / f"{name}.dist-info"
            dist.mkdir()
            (dist / "top_level.txt").write_text(f"{tl}\n")

        result = _build_import_map(tmp_path)
        assert len(result) == 2


# ── Integration: post_process tests ───────────────────────────────────


class TestPythonDependencyLinkerPlugin:
    def test_depends_on_edge_created(self):
        plugin = PythonDependencyLinkerPlugin()
        dep_node = _make_dep_node("requests")
        file_node = _make_file_node("src/api.py")
        nodes = [file_node, dep_node]
        ctx = _make_context({"src/api.py": "import requests\n\ndef fetch():\n    pass\n"})

        _, new_edges = plugin.post_process(nodes, [], ctx)
        depends_on = [e for e in new_edges if e.edge_type == "depends_on"]
        assert len(depends_on) == 1
        assert str(depends_on[0].from_urn) == str(file_node.urn)
        assert str(depends_on[0].to_urn) == str(dep_node.urn)

    def test_no_edge_for_untracked_import(self):
        plugin = PythonDependencyLinkerPlugin()
        dep_node = _make_dep_node("requests")
        file_node = _make_file_node("src/api.py")
        nodes = [file_node, dep_node]
        ctx = _make_context({"src/api.py": "import os\nimport sys\n"})

        _, new_edges = plugin.post_process(nodes, [], ctx)
        depends_on = [e for e in new_edges if e.edge_type == "depends_on"]
        assert len(depends_on) == 0

    def test_multiple_files_same_dependency(self):
        plugin = PythonDependencyLinkerPlugin()
        dep_node = _make_dep_node("requests")
        file1 = _make_file_node("src/api.py")
        file2 = _make_file_node("src/client.py")
        nodes = [file1, file2, dep_node]
        ctx = _make_context({
            "src/api.py": "import requests\n",
            "src/client.py": "import requests\n",
        })

        _, new_edges = plugin.post_process(nodes, [], ctx)
        depends_on = [e for e in new_edges if e.edge_type == "depends_on"]
        assert len(depends_on) == 2

    def test_no_dependency_nodes_no_crash(self):
        plugin = PythonDependencyLinkerPlugin()
        file_node = _make_file_node("src/api.py")
        ctx = _make_context({"src/api.py": "import requests\n"})

        _, edges = plugin.post_process([file_node], [], ctx)
        assert len(edges) == 0

    def test_edge_metadata_has_import_name(self):
        plugin = PythonDependencyLinkerPlugin()
        dep_node = _make_dep_node("requests")
        file_node = _make_file_node("src/api.py")
        nodes = [file_node, dep_node]
        ctx = _make_context({"src/api.py": "import requests\n"})

        _, new_edges = plugin.post_process(nodes, [], ctx)
        depends_on = [e for e in new_edges if e.edge_type == "depends_on"]
        assert depends_on[0].metadata["import_name"] == "requests"

    def test_from_import_detected(self):
        plugin = PythonDependencyLinkerPlugin()
        dep_node = _make_dep_node("flask")
        file_node = _make_file_node("src/app.py")
        nodes = [file_node, dep_node]
        ctx = _make_context({"src/app.py": "from flask import Flask\n"})

        _, new_edges = plugin.post_process(nodes, [], ctx)
        depends_on = [e for e in new_edges if e.edge_type == "depends_on"]
        assert len(depends_on) == 1

    def test_venv_discovery_resolves_mismatch(self, tmp_path):
        """When venv has top_level.txt, resolves Pillow→PIL correctly."""
        # Set up fake venv
        sp = tmp_path / ".venv" / "lib" / "python3.13" / "site-packages"
        dist = sp / "Pillow-12.0.0.dist-info"
        dist.mkdir(parents=True)
        (dist / "top_level.txt").write_text("PIL\n")

        plugin = PythonDependencyLinkerPlugin()
        dep_node = _make_dep_node("Pillow")
        file_node = _make_file_node("src/images.py")
        nodes = [file_node, dep_node]
        ctx = _make_context(
            {"src/images.py": "from PIL import Image\n"},
            root_path=tmp_path,
        )

        _, new_edges = plugin.post_process(nodes, [], ctx)
        depends_on = [e for e in new_edges if e.edge_type == "depends_on"]
        assert len(depends_on) == 1
