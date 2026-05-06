"""IamRoleNode — an AWS IAM role."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.assumes_edge import AssumesEdge
from labyrinth.graph.edges.attaches_edge import AttachesEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class IamRoleNode(Node):
    """An AWS IAM role."""

    node_type: str = NodeType.IAM_ROLE

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset()
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        AssumesEdge,
        AttachesEdge,
    })

    @staticmethod
    def build_urn(account_id: str, role_name: str) -> URN:
        return URN(f"urn:aws:iam:{account_id}::role/{role_name}")

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        role_name: str,
        trust_policy: dict | None = None,
        arn: str | None = None,
    ) -> IamRoleNode:
        meta = NodeMetadata({NK.ROLE_NAME: role_name})
        if trust_policy is not None:
            meta[NK.IAM_TRUST_POLICY] = trust_policy
        if arn is not None:
            meta[NK.ARN] = arn
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
