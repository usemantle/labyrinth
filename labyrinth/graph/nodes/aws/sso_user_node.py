"""SsoUserNode — an AWS IAM Identity Center user."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.assumes_edge import AssumesEdge
from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.member_of_edge import MemberOfEdge
from labyrinth.graph.edges.okta_edges import OktaMapsToEdge
from labyrinth.graph.edges.sso_assigned_to_edge import SsoAssignedToEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class SsoUserNode(Node):
    """An AWS IAM Identity Center user (the AWS-side counterpart of an IdP Person)."""

    node_type: str = NodeType.SSO_USER

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        AssumesEdge,
        MemberOfEdge,
        SsoAssignedToEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        OktaMapsToEdge,
    })

    @staticmethod
    def build_urn(account_id: str, user_id: str) -> URN:
        return URN(f"urn:aws:sso:{account_id}::user/{user_id}")

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
