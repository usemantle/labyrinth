"""UV lockfile plugin for dependency scanning and CVE detection.

Reads ``uv.lock`` (TOML format) from the codebase root, creates a
package_manifest node for the lockfile, then creates dependency nodes
linked via DEPENDS_ON edges from the manifest.  The codebase CONTAINS
the manifest, and the manifest DEPENDS_ON each dependency.

Also creates DEPENDS_ON edges between dependency nodes based on the
``dependencies`` field in each package entry, enabling transitive
vulnerability traversal (e.g. file → python-jose → cryptography [CVE]).
"""

from __future__ import annotations

import logging
import tomllib
from typing import TYPE_CHECKING

from src.graph.edges.contains_edge import ContainsEdge
from src.graph.edges.depends_on_edge import DependsOnEdge
from src.graph.graph_models import (
    Edge,
    Node,
    NodeMetadata,
    NodeMetadataKey,
    NodeType,
)
from src.graph.loaders.codebase.cve.osv_client import query_osv
from src.graph.loaders.codebase.plugins._base import CodebasePlugin
from src.graph.nodes.dependency_node import DependencyNode

if TYPE_CHECKING:
    from src.graph.loaders.codebase.codebase_loader import PostProcessContext

logger = logging.getLogger(__name__)

NK = NodeMetadataKey


class UvPlugin(CodebasePlugin):
    """Scans uv.lock for dependencies and checks CVEs via OSV.dev."""

    @classmethod
    def auto_detect(cls, root_path):
        return (root_path / "uv.lock").exists()

    def supported_languages(self) -> set[str]:
        return {"python"}

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

        # Create the package manifest node (represents uv.lock)
        manifest_urn = context.build_urn(context.root_name, "uv.lock")
        manifest_node = Node(
            organization_id=context.organization_id,
            urn=manifest_urn,
            parent_urn=codebase_urn,
            node_type=NodeType.PACKAGE_MANIFEST,
            metadata=NodeMetadata({
                NK.PACKAGE_MANAGER: "uv",
                NK.MANIFEST_FILE: "uv.lock",
                NK.PACKAGE_ECOSYSTEM: "PyPI",
            }),
        )
        new_nodes.append(manifest_node)
        new_edges.append(ContainsEdge.create(
            context.organization_id,
            codebase_urn,
            manifest_urn,
        ))

        for pkg in packages:
            name = pkg.get("name", "")
            version = pkg.get("version", "")
            if not name or not version:
                continue

            dep_urn = context.build_urn(context.root_name, f"dep/{name}")

            dep_node = DependencyNode.create(
                organization_id=context.organization_id,
                urn=dep_urn,
                parent_urn=manifest_urn,
                package_name=name,
                package_version=version,
                package_ecosystem="PyPI",
            )

            # Query OSV for vulnerabilities
            try:
                result = query_osv(name, version, "PyPI")
                if result.cve_ids:
                    dep_node.metadata[NK.CVE_IDS] = ",".join(result.cve_ids)
            except Exception:
                logger.debug("OSV query failed for %s==%s", name, version)

            new_nodes.append(dep_node)
            new_edges.append(DependsOnEdge.create(
                context.organization_id,
                manifest_urn,
                dep_urn,
            ))

        # Build transitive DEPENDS_ON edges between dependency nodes
        # Map package name → URN for quick lookup
        dep_urn_map = {
            pkg.get("name", ""): context.build_urn(context.root_name, f"dep/{pkg.get('name', '')}")
            for pkg in packages
            if pkg.get("name") and pkg.get("version")
        }

        transitive_count = 0
        for pkg in packages:
            name = pkg.get("name", "")
            if name not in dep_urn_map:
                continue
            from_urn = dep_urn_map[name]
            for dep in pkg.get("dependencies", []):
                dep_name = dep.get("name", "")
                if dep_name in dep_urn_map:
                    new_edges.append(DependsOnEdge.create(
                        context.organization_id,
                        from_urn,
                        dep_urn_map[dep_name],
                    ))
                    transitive_count += 1

        logger.info(
            "UV plugin: found %d packages, %d transitive dependency edges",
            len(new_nodes) - 1,  # subtract manifest node
            transitive_count,
        )
        return nodes + new_nodes, edges + new_edges
