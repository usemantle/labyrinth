"""ApiGatewayNode — an AWS API Gateway REST/HTTP/WebSocket endpoint."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.resolves_to_edge import ResolvesToEdge
from labyrinth.graph.edges.routes_to_edge import RoutesToEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType
from labyrinth.graph.nodes._aws_resource_mixin import AwsResourceMixin

NK = NodeMetadataKey


@dataclass
class ApiGatewayNode(AwsResourceMixin, Node):
    """An AWS API Gateway endpoint (REST, HTTP, or WebSocket)."""

    node_type: str = NodeType.API_GATEWAY

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        RoutesToEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ResolvesToEdge,
        ContainsEdge,
    })

    @classmethod
    def build_urn(cls, region: str, api_id: str) -> URN:
        # API Gateway ARNs don't carry an account id (the API id is globally
        # qualified by region) — use the v2 form ``/apis/`` uniformly.
        return cls.urn_from_arn(f"arn:aws:apigateway:{region}::/apis/{api_id}")

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        api_gw_type: str,
        scheme: str,
        dns_name: str | None = None,
        endpoint_type: str | None = None,
        stage: str | None = None,
        auth_type: str | None = None,
        api_id: str | None = None,
    ) -> ApiGatewayNode:
        meta = NodeMetadata({
            NK.LB_TYPE: api_gw_type,
            NK.LB_SCHEME: scheme,
        })
        if dns_name is not None:
            meta[NK.LB_DNS_NAME] = dns_name
        if endpoint_type is not None:
            meta[NK.API_GW_ENDPOINT_TYPE] = endpoint_type
        if stage is not None:
            meta[NK.API_GW_STAGE] = stage
        if auth_type is not None:
            meta[NK.API_GW_AUTH_TYPE] = auth_type
        if api_id is not None:
            meta[NK.ARN] = api_id
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
