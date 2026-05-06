"""IamUserNode — an AWS IAM user."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.assumes_edge import AssumesEdge
from labyrinth.graph.edges.attaches_edge import AttachesEdge
from labyrinth.graph.edges.member_of_edge import MemberOfEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class IamUserNode(Node):
    """An AWS IAM user."""

    node_type: str = NodeType.IAM_USER

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        AssumesEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        AttachesEdge,
        MemberOfEdge,
    })

    @staticmethod
    def build_urn(account_id: str, user_name: str) -> URN:
        return URN(f"urn:aws:iam:{account_id}::user/{user_name}")

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        user_name: str,
        access_keys: list[dict] | None = None,
        mfa_enabled: bool | None = None,
        last_activity: str | None = None,
        arn: str | None = None,
    ) -> IamUserNode:
        meta = NodeMetadata({NK.IAM_USER_NAME: user_name})
        if access_keys is not None:
            meta[NK.IAM_ACCESS_KEYS] = access_keys
        if mfa_enabled is not None:
            meta[NK.IAM_MFA_ENABLED] = mfa_enabled
        if last_activity is not None:
            meta[NK.IAM_LAST_ACTIVITY] = last_activity
        if arn is not None:
            meta[NK.ARN] = arn
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
