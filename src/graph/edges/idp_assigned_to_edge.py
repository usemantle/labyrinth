"""IdpAssignedToEdge — a Person or Group is assigned to an Application in an IdP."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from src.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata, EdgeType


@dataclass
class IdpAssignedToEdge(Edge):
    """A Person or Group assigned to an Application in an IdP (e.g. Okta)."""

    edge_type: str = EdgeType.IDP_ASSIGNED_TO

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        metadata: EdgeMetadata | None = None,
    ) -> IdpAssignedToEdge:
        edge_uuid = uuid.uuid5(
            EDGE_NAMESPACE,
            f"{from_urn}:{to_urn}:idp:assigned_to",
        )
        return cls(
            uuid=edge_uuid,
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=metadata or EdgeMetadata(),
        )
