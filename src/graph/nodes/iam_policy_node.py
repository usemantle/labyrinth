"""IamPolicyNode — an AWS IAM policy."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from src.graph.edges.attaches_edge import AttachesEdge
from src.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class IamPolicyNode(Node):
    """An AWS IAM policy."""

    node_type: str = NodeType.IAM_POLICY

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        AttachesEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset()

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
