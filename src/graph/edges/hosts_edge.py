"""HostsEdge — infrastructure hosting a data resource."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from src.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata, RelationType


@dataclass
class HostsEdge(Edge):
    """Infrastructure resource hosting a data resource (e.g., RDS -> database)."""

    relation_type: RelationType = RelationType.HOSTS
    edge_type: str = "hosts"

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        metadata: EdgeMetadata | None = None,
    ) -> HostsEdge:
        edge_uuid = uuid.uuid5(
            EDGE_NAMESPACE,
            f"{from_urn}:{to_urn}:{RelationType.HOSTS.value}",
        )
        return cls(
            uuid=edge_uuid,
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=metadata or EdgeMetadata(),
        )
