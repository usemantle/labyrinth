"""SsoUserNode — an AWS IAM Identity Center user."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from src.graph.edges.contains_edge import ContainsEdge
from src.graph.edges.idp_maps_to_edge import IdpMapsToEdge
from src.graph.edges.member_of_edge import MemberOfEdge
from src.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class SsoUserNode(Node):
    """An AWS IAM Identity Center user (the AWS-side counterpart of an IdP Person)."""

    node_type: str = NodeType.SSO_USER

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        MemberOfEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        IdpMapsToEdge,
    })

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        user_id: str,
        user_name: str | None = None,
        email: str | None = None,
        external_id: str | None = None,
    ) -> SsoUserNode:
        meta = NodeMetadata({NK.SSO_USER_ID: user_id})
        if user_name is not None:
            meta[NK.SSO_USER_NAME] = user_name
        if email is not None:
            meta[NK.SSO_USER_EMAIL] = email
        if external_id is not None:
            meta[NK.SSO_USER_EXTERNAL_ID] = external_id
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
