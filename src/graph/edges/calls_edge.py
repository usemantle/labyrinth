"""CallsEdge — function-to-function call relationship."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from src.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata


@dataclass
class CallsEdge(Edge):
    """A function calling another function."""

    edge_type: str = "calls"

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        metadata: EdgeMetadata | None = None,
    ) -> CallsEdge:
        edge_uuid = uuid.uuid5(
            EDGE_NAMESPACE,
            f"{from_urn}:{to_urn}:CODE_TO_CODE",
        )
        return cls(
            uuid=edge_uuid,
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=metadata or EdgeMetadata(),
        )
