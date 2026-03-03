"""UV lockfile plugin for dependency scanning and CVE detection.

Reads ``uv.lock`` (TOML format) from the codebase root, creates a node
for each package with PACKAGE_NAME, PACKAGE_VERSION, and
PACKAGE_ECOSYSTEM metadata, links them via CONTAINS edges to the
codebase root, and queries OSV.dev for known vulnerabilities.
"""

from __future__ import annotations

import logging
import tomllib
from typing import TYPE_CHECKING

from src.graph.graph_models import (
    Edge,
    Node,
    NodeMetadata,
    NodeMetadataKey,
    RelationType,
)
from src.graph.loaders._helpers import make_edge
from src.graph.loaders.codebase.cve.osv_client import query_osv
from src.graph.loaders.codebase.plugins._base import CodebasePlugin

if TYPE_CHECKING:
    from src.graph.loaders.codebase.codebase_loader import PostProcessContext

logger = logging.getLogger(__name__)


class UvPlugin(CodebasePlugin):
    """Scans uv.lock for dependencies and checks CVEs via OSV.dev."""

    def post_process(
        self,
        nodes: list[Node],
        edges: list[Edge],
        context: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        lock_path = context.root_path / "uv.lock"
        if not lock_path.exists():
            logger.debug("No uv.lock found at %s, skipping", lock_path)
            return nodes, edges

        try:
            with open(lock_path, "rb") as f:
                lock_data = tomllib.load(f)
        except Exception:
            logger.warning("Failed to parse uv.lock at %s", lock_path)
            return nodes, edges

        packages = lock_data.get("package", [])
        if not packages:
            return nodes, edges

        # Find codebase root URN
        codebase_urn = context.build_urn(context.root_name)

        new_nodes: list[Node] = []
        new_edges: list[Edge] = []

        for pkg in packages:
            name = pkg.get("name", "")
            version = pkg.get("version", "")
            if not name or not version:
                continue

            dep_urn = context.build_urn(context.root_name, f"dep/{name}")

            meta = NodeMetadata({
                NodeMetadataKey.PACKAGE_NAME: name,
                NodeMetadataKey.PACKAGE_VERSION: version,
                NodeMetadataKey.PACKAGE_ECOSYSTEM: "PyPI",
            })

            # Query OSV for vulnerabilities
            try:
                result = query_osv(name, version, "PyPI")
                if result.cve_ids:
                    meta[NodeMetadataKey.CVE_IDS] = ",".join(result.cve_ids)
            except Exception:
                logger.debug("OSV query failed for %s==%s", name, version)

            dep_node = Node(
                organization_id=context.organization_id,
                urn=dep_urn,
                parent_urn=codebase_urn,
                metadata=meta,
            )
            new_nodes.append(dep_node)
            new_edges.append(make_edge(
                context.organization_id,
                codebase_urn,
                dep_urn,
                RelationType.CONTAINS,
            ))

        logger.info("UV plugin: found %d packages in uv.lock", len(new_nodes))
        return nodes + new_nodes, edges + new_edges
