"""IdpPushesToEdge — an IdP-managed Group is pushed to a downstream Application."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from src.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata, EdgeType


@dataclass
class IdpPushesToEdge(Edge):
    """A Group whose membership is pushed by the IdP to a downstream Application
    (e.g. Okta Group Push to AWS, Snowflake, Salesforce)."""

    edge_type: str = EdgeType.IDP_PUSHES_TO

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        metadata: EdgeMetadata | None = None,
    ) -> IdpPushesToEdge:
        edge_uuid = uuid.uuid5(
            EDGE_NAMESPACE,
            f"{from_urn}:{to_urn}:idp:pushes_to",
        )
        return cls(
            uuid=edge_uuid,
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=metadata or EdgeMetadata(),
        )
