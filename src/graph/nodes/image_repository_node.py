"""ImageRepositoryNode — a container image repository (e.g., ECR repo)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from src.graph.edges.builds_edge import BuildsEdge
from src.graph.edges.contains_edge import ContainsEdge
from src.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class ImageRepositoryNode(Node):
    """A container image repository (e.g., AWS ECR repository)."""

    node_type: str = NodeType.IMAGE_REPOSITORY

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        BuildsEdge,
    })

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        repository_name: str,
        repository_uri: str | None = None,
        arn: str | None = None,
        account_id: str | None = None,
        region: str | None = None,
    ) -> ImageRepositoryNode:
        meta = NodeMetadata({NK.REPOSITORY_NAME: repository_name})
        if repository_uri is not None:
            meta[NK.REPOSITORY_URI] = repository_uri
        if arn is not None:
            meta[NK.ARN] = arn
        if account_id is not None:
            meta[NK.ACCOUNT_ID] = account_id
        if region is not None:
            meta[NK.REGION] = region
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
