"""InstantiatesEdge — function-to-class instantiation relationship."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from src.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata


@dataclass
class InstantiatesEdge(Edge):
    """A function instantiating a class."""

    edge_type: str = "instantiates"

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        metadata: EdgeMetadata | None = None,
    ) -> InstantiatesEdge:
        edge_uuid = uuid.uuid5(
            EDGE_NAMESPACE,
            f"{from_urn}:{to_urn}:instantiates",
        )
        return cls(
            uuid=edge_uuid,
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=metadata or EdgeMetadata(),
        )
