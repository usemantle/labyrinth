"""LoadBalancerNode — an AWS ALB or NLB."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.protected_by_edge import ProtectedByEdge
from labyrinth.graph.edges.resolves_to_edge import ResolvesToEdge
from labyrinth.graph.edges.routes_to_edge import RoutesToEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType
from labyrinth.graph.nodes._aws_resource_mixin import AwsResourceMixin

NK = NodeMetadataKey


@dataclass
class LoadBalancerNode(AwsResourceMixin, Node):
    """An AWS Elastic Load Balancer (ALB or NLB)."""

    node_type: str = NodeType.LOAD_BALANCER

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        RoutesToEdge,
        ProtectedByEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ResolvesToEdge,
        ContainsEdge,
    })

    @classmethod
    def build_urn(cls, account_id: str, region: str, lb_name: str) -> URN:
        return cls.urn_from_arn(
            f"arn:aws:elasticloadbalancing:{region}:{account_id}:loadbalancer/{lb_name}",
        )

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        lb_type: str,
        lb_scheme: str,
        lb_dns_name: str | None = None,
        listeners: list[dict] | None = None,
        lb_state: str | None = None,
        arn: str | None = None,
    ) -> LoadBalancerNode:
        meta = NodeMetadata({
            NK.LB_TYPE: lb_type,
            NK.LB_SCHEME: lb_scheme,
        })
        if lb_dns_name is not None:
            meta[NK.LB_DNS_NAME] = lb_dns_name
        if listeners is not None:
            meta[NK.LB_LISTENERS] = listeners
        if lb_state is not None:
            meta[NK.LB_STATE] = lb_state
        if arn is not None:
            meta[NK.ARN] = arn
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
