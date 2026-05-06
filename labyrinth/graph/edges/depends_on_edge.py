"""DependsOnEdge — dependency relationship between packages or files."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from labyrinth.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata, EdgeType


@dataclass
class DependsOnEdge(Edge):
    """A dependency relationship (e.g., file depends on package)."""

    edge_type: str = EdgeType.DEPENDS_ON

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        metadata: EdgeMetadata | None = None,
    ) -> DependsOnEdge:
        edge_uuid = uuid.uuid5(
            EDGE_NAMESPACE,
            f"{from_urn}:{to_urn}:DEPENDS_ON",
        )
        return cls(
            uuid=edge_uuid,
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=metadata or EdgeMetadata(),
        )
