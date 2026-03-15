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
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset()

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        account_id: str,
        region: str,
    ) -> AwsAccountNode:
        meta = NodeMetadata({
            NK.ACCOUNT_ID: account_id,
            NK.REGION: region,
        })
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
