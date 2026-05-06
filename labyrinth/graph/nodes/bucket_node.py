"""BucketNode — an S3 bucket or equivalent object store container."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType
from labyrinth.graph.nodes._aws_resource_mixin import AwsResourceMixin

NK = NodeMetadataKey


@dataclass
class BucketNode(AwsResourceMixin, Node):
    """An AWS S3 bucket."""

    node_type: str = NodeType.S3_BUCKET

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset()

    @classmethod
    def build_urn(cls, bucket_name: str) -> URN:
        return cls.urn_from_arn(f"arn:aws:s3:::{bucket_name}")

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
