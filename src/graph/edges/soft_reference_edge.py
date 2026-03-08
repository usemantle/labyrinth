"""SoftReferenceEdge — implicit/inferred data-to-data reference."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from src.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata, RelationType


@dataclass
class SoftReferenceEdge(Edge):
    """An implicit or inferred reference between data nodes."""

    relation_type: RelationType = RelationType.DATA_TO_DATA
    edge_type: str = "soft_reference"

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        metadata: EdgeMetadata | None = None,
    ) -> SoftReferenceEdge:
        edge_uuid = uuid.uuid5(
            EDGE_NAMESPACE,
            f"{from_urn}:{to_urn}:soft_reference",
        )
        return cls(
            uuid=edge_uuid,
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=metadata or EdgeMetadata(),
        )
