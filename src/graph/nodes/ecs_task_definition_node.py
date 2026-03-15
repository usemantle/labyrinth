"""EcsTaskDefinitionNode — an ECS task definition."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from src.graph.edges.assumes_edge import AssumesEdge
from src.graph.edges.references_edge import ReferencesEdge
from src.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class EcsTaskDefinitionNode(Node):
    """An AWS ECS task definition."""

    node_type: str = NodeType.ECS_TASK_DEFINITION

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        AssumesEdge,
        ReferencesEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ReferencesEdge,
    })

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        family: str,
        revision: int | None = None,
        container_images: list[str] | None = None,
        task_role_arn: str | None = None,
        execution_role_arn: str | None = None,
        arn: str | None = None,
    ) -> EcsTaskDefinitionNode:
        meta = NodeMetadata({NK.ECS_TASK_FAMILY: family})
        if revision is not None:
            meta[NK.ECS_TASK_REVISION] = revision
        if container_images is not None:
            meta[NK.ECS_CONTAINER_IMAGES] = container_images
        if task_role_arn is not None:
            meta[NK.ECS_TASK_ROLE_ARN] = task_role_arn
        if execution_role_arn is not None:
            meta[NK.ECS_EXECUTION_ROLE_ARN] = execution_role_arn
        if arn is not None:
            meta[NK.ARN] = arn
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
