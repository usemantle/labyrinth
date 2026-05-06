"""IamPolicyNode — an AWS IAM policy."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.attaches_edge import AttachesEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class IamPolicyNode(Node):
    """An AWS IAM policy."""

    node_type: str = NodeType.IAM_POLICY

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        AttachesEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset()

    @staticmethod
    def build_urn(
        account_id: str,
        policy_name: str,
        *,
        aws_managed: bool = False,
    ) -> URN:
        prefix = "policy/aws" if aws_managed else "policy"
        return URN(f"urn:aws:iam:{account_id}::{prefix}/{policy_name}")

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        policy_name: str,
        policy_arn: str | None = None,
        policy_document: dict | None = None,
    ) -> IamPolicyNode:
        meta = NodeMetadata({NK.IAM_POLICY_NAME: policy_name})
        if policy_arn is not None:
            meta[NK.IAM_POLICY_ARN] = policy_arn
        if policy_document is not None:
            meta[NK.IAM_POLICY_DOCUMENT] = policy_document
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
