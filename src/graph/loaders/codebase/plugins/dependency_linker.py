"""Dependency linker plugin for connecting code files to dependency nodes.

After the UV plugin creates dependency nodes with PACKAGE_NAME metadata,
this plugin scans Python file sources for import statements and creates
DEPENDS_ON edges from file nodes to matching dependency nodes.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from src.graph.graph_models import (
    Edge,
    EdgeMetadata,
    EdgeMetadataKey,
    Node,
    NodeMetadataKey,
    RelationType,
)
from src.graph.loaders._helpers import make_edge
from src.graph.loaders.codebase.plugins._base import CodebasePlugin

if TYPE_CHECKING:
    from src.graph.loaders.codebase.codebase_loader import PostProcessContext

logger = logging.getLogger(__name__)

NK = NodeMetadataKey
EK = EdgeMetadataKey

# Known PyPI package → import name mismatches
_PACKAGE_TO_IMPORT: dict[str, str] = {
    "pillow": "PIL",
    "python-dateutil": "dateutil",
    "beautifulsoup4": "bs4",
    "scikit-learn": "sklearn",
    "pyyaml": "yaml",
    "python-dotenv": "dotenv",
    "python-jose": "jose",
    "python-multipart": "multipart",
    "opencv-python": "cv2",
    "attrs": "attr",
}

# Regex for top-level imports
_IMPORT_RE = re.compile(r"^import\s+(\w+)", re.MULTILINE)
_FROM_IMPORT_RE = re.compile(r"^from\s+(\w+)", re.MULTILINE)


def _normalize_package_to_import(package_name: str) -> str:
    """Convert a PyPI package name to its Python import name."""
    lower = package_name.lower()
    if lower in _PACKAGE_TO_IMPORT:
        return _PACKAGE_TO_IMPORT[lower]
    return lower.replace("-", "_")


class DependencyLinkerPlugin(CodebasePlugin):
    """Links code files to dependency nodes via DEPENDS_ON edges."""

    def post_process(
        self,
        nodes: list[Node],
        edges: list[Edge],
        context: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        # Step 1: Collect dependency nodes
        dep_map: dict[str, Node] = {}
        for node in nodes:
            pkg_name = node.metadata.get(NK.PACKAGE_NAME)
            if pkg_name:
                import_name = _normalize_package_to_import(pkg_name)
                dep_map[import_name.lower()] = node

        if not dep_map:
            return nodes, edges

        # Step 2: Build file URN lookup
        file_urn_map: dict[str, str] = {}
        for node in nodes:
            fp = node.metadata.get(NK.FILE_PATH)
            if fp and NK.FUNCTION_NAME not in node.metadata and NK.CLASS_NAME not in node.metadata:
                file_urn_map[fp] = str(node.urn)

        # Step 3: Scan Python files for imports
        new_edges: list[Edge] = []
        linked = 0

        for rel_path, source in context.file_sources.items():
            lang = context.file_languages.get(rel_path)
            if lang != "python":
                continue

            file_urn_str = file_urn_map.get(rel_path)
            if not file_urn_str:
                continue

            # Extract top-level import names
            import_names: set[str] = set()
            for match in _IMPORT_RE.finditer(source):
                import_names.add(match.group(1).lower())
            for match in _FROM_IMPORT_RE.finditer(source):
                import_names.add(match.group(1).lower())

            # Match against dependency nodes
            for import_name in import_names:
                dep_node = dep_map.get(import_name)
                if dep_node:
                    from src.graph.graph_models import URN
                    edge = make_edge(
                        context.organization_id,
                        URN(file_urn_str),
                        dep_node.urn,
                        RelationType.DEPENDS_ON,
                        metadata=EdgeMetadata({
                            EK.IMPORT_NAME: import_name,
                        }),
                    )
                    new_edges.append(edge)
                    linked += 1

        logger.info("Dependency linker: created %d DEPENDS_ON edges", linked)
        return nodes, edges + new_edges
