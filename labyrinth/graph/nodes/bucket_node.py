"""BucketNode — an S3 bucket or equivalent object store container."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class BucketNode(Node):
    """An S3 bucket or equivalent object store container."""

    node_type: str = NodeType.S3_BUCKET

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset()

    @staticmethod
    def build_urn(account_id: str, region: str, bucket_name: str) -> URN:
        return URN(f"urn:aws:s3:{account_id}:{region}:{bucket_name}")

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        bucket_name: str,
        arn: str | None = None,
        account_id: str | None = None,
        region: str | None = None,
    ) -> BucketNode:
        meta = NodeMetadata({NK.BUCKET_NAME: bucket_name})
        if arn is not None:
            meta[NK.ARN] = arn
        if account_id is not None:
            meta[NK.ACCOUNT_ID] = account_id
        if region is not None:
            meta[NK.REGION] = region
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
