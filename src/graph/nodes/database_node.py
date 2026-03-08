"""DatabaseNode — a database instance."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from src.graph.edges.contains_edge import ContainsEdge
from src.graph.edges.hosts_edge import HostsEdge
from src.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey

NK = NodeMetadataKey


@dataclass
class DatabaseNode(Node):
    """A database instance (e.g., PostgreSQL, MySQL)."""

    node_type: str = "database"

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        HostsEdge,
    })

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        database_name: str,
        host: str | None = None,
        port: int | None = None,
    ) -> DatabaseNode:
        meta = NodeMetadata({NK.DATABASE_NAME: database_name})
        if host is not None:
            meta[NK.HOST] = host
        if port is not None:
            meta[NK.PORT] = port
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
