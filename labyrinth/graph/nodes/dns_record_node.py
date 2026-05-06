"""DnsRecordNode — a DNS record (Route53 A/AAAA/CNAME)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.resolves_to_edge import ResolvesToEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType
from labyrinth.graph.nodes._aws_resource_mixin import AwsResourceMixin

NK = NodeMetadataKey


@dataclass
class DnsRecordNode(AwsResourceMixin, Node):
    """A Route53 DNS record."""

    node_type: str = NodeType.DNS_RECORD

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        ResolvesToEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })

    @classmethod
    def build_urn(
        cls,
        zone_id: str,
        record_name: str,
        record_type: str,
    ) -> URN:
        # Route53 records have no real ARN; synthesise one off the hosted-zone ARN.
        return cls.urn_from_arn(
            f"arn:aws:route53:::hostedzone/{zone_id}/recordset/{record_name}/{record_type}",
        )

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        record_name: str,
        record_type: str,
        zone_name: str | None = None,
        zone_private: bool | None = None,
        zone_id: str | None = None,
        ttl: int | None = None,
        values: list[str] | None = None,
    ) -> DnsRecordNode:
        meta = NodeMetadata({
            NK.DNS_RECORD_NAME: record_name,
            NK.DNS_RECORD_TYPE: record_type,
        })
        if zone_name is not None:
            meta[NK.DNS_ZONE_NAME] = zone_name
        if zone_private is not None:
            meta[NK.DNS_ZONE_PRIVATE] = zone_private
        if zone_id is not None:
            meta[NK.DNS_ZONE_ID] = zone_id
        if ttl is not None:
            meta[NK.DNS_TTL] = ttl
        if values is not None:
            meta[NK.DNS_VALUES] = values
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
