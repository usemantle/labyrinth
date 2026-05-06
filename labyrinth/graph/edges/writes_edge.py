"""WritesEdge — code or principal writing to a data resource."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from labyrinth.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata, EdgeType


@dataclass
class WritesEdge(Edge):
    """A function or principal writing to a data resource."""

    edge_type: str = EdgeType.WRITES

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        metadata: EdgeMetadata | None = None,
    ) -> WritesEdge:
        edge_uuid = uuid.uuid5(
            EDGE_NAMESPACE,
            f"{from_urn}:{to_urn}:writes",
        )
        return cls(
            uuid=edge_uuid,
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=metadata or EdgeMetadata(),
        )
