"""RoutesToEdge — traffic routing between networking components."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from labyrinth.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata, EdgeType


@dataclass
class RoutesToEdge(Edge):
    """Traffic routing (e.g., LB -> target group, target group -> ECS service)."""

    edge_type: str = EdgeType.ROUTES_TO

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        metadata: EdgeMetadata | None = None,
    ) -> RoutesToEdge:
        edge_uuid = uuid.uuid5(
            EDGE_NAMESPACE,
            f"{from_urn}:{to_urn}:routes_to",
        )
        return cls(
            uuid=edge_uuid,
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=metadata or EdgeMetadata(),
        )
