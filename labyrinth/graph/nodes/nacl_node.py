"""NaclNode — an AWS network ACL."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType
from labyrinth.graph.nodes._aws_resource_mixin import AwsResourceMixin

NK = NodeMetadataKey


@dataclass
class NaclNode(AwsResourceMixin, Node):
    """An AWS network ACL associated with a VPC subnet."""

    node_type: str = NodeType.NACL

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset()
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })

    @classmethod
    def build_urn(cls, account_id: str, region: str, nacl_id: str) -> URN:
        return cls.urn_from_arn(
            f"arn:aws:ec2:{region}:{account_id}:network-acl/{nacl_id}",
        )

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        nacl_id: str,
        rules: list[dict] | None = None,
        vpc_id: str | None = None,
    ) -> NaclNode:
        meta = NodeMetadata({NK.NACL_ID: nacl_id})
        if rules is not None:
            meta[NK.NACL_RULES] = rules
        if vpc_id is not None:
            meta[NK.VPC_ID] = vpc_id
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
