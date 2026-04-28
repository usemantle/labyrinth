"""IdpPartOfEdge — a Person belongs to a Group in an IdP."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from src.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata, EdgeType


@dataclass
class IdpPartOfEdge(Edge):
    """A Person who is part of a Group in an IdP (e.g. Okta)."""

    edge_type: str = EdgeType.IDP_PART_OF

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        metadata: EdgeMetadata | None = None,
    ) -> IdpPartOfEdge:
        edge_uuid = uuid.uuid5(
            EDGE_NAMESPACE,
            f"{from_urn}:{to_urn}:idp:part_of",
        )
        return cls(
            uuid=edge_uuid,
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=metadata or EdgeMetadata(),
        )
