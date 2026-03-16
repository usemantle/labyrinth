"""LoadBalancerNode — ALB, NLB, or API Gateway endpoint."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from src.graph.edges.contains_edge import ContainsEdge
from src.graph.edges.protected_by_edge import ProtectedByEdge
from src.graph.edges.resolves_to_edge import ResolvesToEdge
from src.graph.edges.routes_to_edge import RoutesToEdge
from src.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class LoadBalancerNode(Node):
    """A load balancer (ALB, NLB) or API Gateway endpoint."""

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
        api_gw_stage: str | None = None,
        api_gw_endpoint_type: str | None = None,
        api_gw_auth_type: str | None = None,
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
        if api_gw_stage is not None:
            meta[NK.API_GW_STAGE] = api_gw_stage
        if api_gw_endpoint_type is not None:
            meta[NK.API_GW_ENDPOINT_TYPE] = api_gw_endpoint_type
        if api_gw_auth_type is not None:
            meta[NK.API_GW_AUTH_TYPE] = api_gw_auth_type
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
