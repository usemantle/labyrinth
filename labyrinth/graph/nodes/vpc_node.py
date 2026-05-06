"""VpcNode — an AWS VPC."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType
from labyrinth.graph.nodes._aws_resource_mixin import AwsResourceMixin

NK = NodeMetadataKey


@dataclass
class VpcNode(AwsResourceMixin, Node):
    """An AWS VPC."""

    node_type: str = NodeType.VPC

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })

    @classmethod
    def build_urn(cls, account_id: str, region: str, vpc_id: str) -> URN:
        return cls.urn_from_arn(f"arn:aws:ec2:{region}:{account_id}:vpc/{vpc_id}")

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        vpc_id: str,
        cidr: str | None = None,
        arn: str | None = None,
    ) -> VpcNode:
        meta = NodeMetadata({NK.VPC_ID: vpc_id})
        if cidr is not None:
            meta[NK.VPC_CIDR] = cidr
        if arn is not None:
            meta[NK.ARN] = arn
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
