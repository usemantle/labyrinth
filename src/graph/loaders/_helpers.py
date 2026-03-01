"""Shared helpers for graph loaders."""

import uuid

from src.graph.graph_models import Edge, EdgeMetadata, RelationType, URN

EDGE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "dsec:graph:edge")


def make_edge(
    organization_id: uuid.UUID,
    from_urn: URN,
    to_urn: URN,
    relation_type: RelationType,
    metadata: EdgeMetadata | None = None,
) -> Edge:
    """Create an Edge with a deterministic UUID."""
    edge_uuid = uuid.uuid5(
        EDGE_NAMESPACE,
        f"{from_urn}:{to_urn}:{relation_type.value}",
    )
    return Edge(
        uuid=edge_uuid,
        organization_id=organization_id,
        from_urn=from_urn,
        to_urn=to_urn,
        relation_type=relation_type,
        metadata=metadata or EdgeMetadata(),
    )
