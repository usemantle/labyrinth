"""ModelsEdge — ORM class modeling a data resource."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from src.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata


@dataclass
class ModelsEdge(Edge):
    """An ORM class modeling a data resource (e.g., SQLAlchemy model -> table)."""

    edge_type: str = "models"

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        metadata: EdgeMetadata | None = None,
    ) -> ModelsEdge:
        edge_uuid = uuid.uuid5(
            EDGE_NAMESPACE,
            f"{from_urn}:{to_urn}:models",
        )
        return cls(
            uuid=edge_uuid,
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=metadata or EdgeMetadata(),
        )
