"""ReadsEdge — code or principal reading from a data resource."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from src.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata


@dataclass
class ReadsEdge(Edge):
    """A function or principal reading from a data resource."""

    edge_type: str = "reads"

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        metadata: EdgeMetadata | None = None,
    ) -> ReadsEdge:
        edge_uuid = uuid.uuid5(
            EDGE_NAMESPACE,
            f"{from_urn}:{to_urn}:reads",
        )
        return cls(
            uuid=edge_uuid,
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=metadata or EdgeMetadata(),
        )
