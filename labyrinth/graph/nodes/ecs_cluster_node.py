"""EcsClusterNode — an ECS cluster."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType
from labyrinth.graph.nodes._aws_resource_mixin import AwsResourceMixin

NK = NodeMetadataKey


@dataclass
class EcsClusterNode(AwsResourceMixin, Node):
    """An AWS ECS cluster."""

    node_type: str = NodeType.ECS_CLUSTER

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })

    @classmethod
    def build_urn(cls, account_id: str, region: str, cluster_name: str) -> URN:
        return cls.urn_from_arn(
            f"arn:aws:ecs:{region}:{account_id}:cluster/{cluster_name}",
        )

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        cluster_name: str,
        arn: str | None = None,
    ) -> EcsClusterNode:
        meta = NodeMetadata({NK.ECS_CLUSTER_NAME: cluster_name})
        if arn is not None:
            meta[NK.ARN] = arn
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
