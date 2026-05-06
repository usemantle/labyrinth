"""ExecutesEdge — a Dockerfile that executes a code file as its container entrypoint."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from labyrinth.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata, EdgeType


@dataclass
class ExecutesEdge(Edge):
    """A Dockerfile that executes a code file as its container entrypoint."""

    edge_type: str = EdgeType.EXECUTES

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        metadata: EdgeMetadata | None = None,
    ) -> ExecutesEdge:
        edge_uuid = uuid.uuid5(
            EDGE_NAMESPACE,
            f"{from_urn}:{to_urn}:executes",
        )
        return cls(
            uuid=edge_uuid,
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=metadata or EdgeMetadata(),
        )
