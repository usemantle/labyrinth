"""ObjectPathNode — an S3 prefix or object path."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from src.graph.edges.contains_edge import ContainsEdge
from src.graph.edges.reads_edge import ReadsEdge
from src.graph.edges.writes_edge import WritesEdge
from src.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class ObjectPathNode(Node):
    """An S3 prefix or object path within a bucket."""

    node_type: str = NodeType.S3_PREFIX

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        ReadsEdge,
        WritesEdge,
    })

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        path_pattern: str,
        object_count: int | None = None,
        sample_keys: str | None = None,
    ) -> ObjectPathNode:
        meta = NodeMetadata({NK.PATH_PATTERN: path_pattern})
        if object_count is not None:
            meta[NK.OBJECT_COUNT] = object_count
        if sample_keys is not None:
            meta[NK.SAMPLE_KEYS] = sample_keys
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
