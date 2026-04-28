"""IdpMapsToEdge — an IdP identity mapped to an identity in another system."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from src.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata, EdgeType


@dataclass
class IdpMapsToEdge(Edge):
    """An IdP identity (e.g. an Okta Person) mapped to a downstream identity (e.g. an AWS SsoUser)."""

    edge_type: str = EdgeType.IDP_MAPS_TO

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        metadata: EdgeMetadata | None = None,
    ) -> IdpMapsToEdge:
        edge_uuid = uuid.uuid5(
            EDGE_NAMESPACE,
            f"{from_urn}:{to_urn}:idp:maps_to",
        )
        return cls(
            uuid=edge_uuid,
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=metadata or EdgeMetadata(),
        )
