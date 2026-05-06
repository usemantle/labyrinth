"""DependencyNode — a third-party package dependency."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.depends_on_edge import DependsOnEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class DependencyNode(Node):
    """A third-party package from UV, Cargo, Gradle, etc."""

    node_type: str = NodeType.DEPENDENCY

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        DependsOnEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        DependsOnEdge,
    })

    @staticmethod
    def build_urn(codebase_urn: URN, package_name: str) -> URN:
        return URN(f"{codebase_urn}/dep/{package_name}")

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        package_name: str,
        package_version: str | None = None,
        package_ecosystem: str | None = None,
    ) -> DependencyNode:
        meta = NodeMetadata({NK.PACKAGE_NAME: package_name})
        if package_version is not None:
            meta[NK.PACKAGE_VERSION] = package_version
        if package_ecosystem is not None:
            meta[NK.PACKAGE_ECOSYSTEM] = package_ecosystem
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
