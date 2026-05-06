"""SecurityGroupNode — an AWS security group."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.allows_traffic_to_edge import AllowsTrafficToEdge
from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.protected_by_edge import ProtectedByEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class SecurityGroupNode(Node):
    """An AWS security group with ingress/egress rules."""

    node_type: str = NodeType.SECURITY_GROUP

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        AllowsTrafficToEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        ProtectedByEdge,
        AllowsTrafficToEdge,
    })

    @staticmethod
    def build_urn(
        account_id: str,
        region: str,
        vpc_id: str,
        sg_id: str,
    ) -> URN:
        return URN(f"urn:aws:vpc:{account_id}:{region}:{vpc_id}/sg/{sg_id}")

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        sg_id: str,
        sg_name: str | None = None,
        rules_ingress: list[dict] | None = None,
        rules_egress: list[dict] | None = None,
        vpc_id: str | None = None,
    ) -> SecurityGroupNode:
        meta = NodeMetadata({NK.SG_ID: sg_id})
        if sg_name is not None:
            meta[NK.SG_NAME] = sg_name
        if rules_ingress is not None:
            meta[NK.SG_RULES_INGRESS] = rules_ingress
        if rules_egress is not None:
            meta[NK.SG_RULES_EGRESS] = rules_egress
        if vpc_id is not None:
            meta[NK.VPC_ID] = vpc_id
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
