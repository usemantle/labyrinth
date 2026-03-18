"""AwsAccountNode — an AWS account root container."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from src.graph.edges.contains_edge import ContainsEdge
from src.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class AwsAccountNode(Node):
    """An AWS account serving as the root container for all discovered resources."""

    node_type: str = NodeType.AWS_ACCOUNT

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        account_id: str,
        region: str = "",
        account_name: str | None = None,
        account_email: str | None = None,
        account_status: str | None = None,
        account_joined_method: str | None = None,
    ) -> AwsAccountNode:
        meta = NodeMetadata({
            NK.ACCOUNT_ID: account_id,
            NK.REGION: region,
        })
        if account_name is not None:
            meta[NK.ACCOUNT_NAME] = account_name
        if account_email is not None:
            meta[NK.ACCOUNT_EMAIL] = account_email
        if account_status is not None:
            meta[NK.ACCOUNT_STATUS] = account_status
        if account_joined_method is not None:
            meta[NK.ACCOUNT_JOINED_METHOD] = account_joined_method
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
