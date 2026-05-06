"""SsoGroupNode — an AWS SSO / Identity Center group."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.assumes_edge import AssumesEdge
from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.member_of_edge import MemberOfEdge
from labyrinth.graph.edges.sso_assigned_to_edge import SsoAssignedToEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class SsoGroupNode(Node):
    """An AWS SSO / Identity Center group."""

    node_type: str = NodeType.SSO_GROUP

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        AssumesEdge,
        MemberOfEdge,
        SsoAssignedToEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        MemberOfEdge,
    })

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        group_id: str,
        group_name: str | None = None,
    ) -> SsoGroupNode:
        meta = NodeMetadata({NK.SSO_GROUP_ID: group_id})
        if group_name is not None:
            meta[NK.SSO_GROUP_NAME] = group_name
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
