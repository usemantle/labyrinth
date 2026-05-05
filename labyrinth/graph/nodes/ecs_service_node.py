"""EcsServiceNode — an ECS service."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.assumes_edge import AssumesEdge
from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.protected_by_edge import ProtectedByEdge
from labyrinth.graph.edges.references_edge import ReferencesEdge
from labyrinth.graph.edges.routes_to_edge import RoutesToEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class EcsServiceNode(Node):
    """An AWS ECS service running within a cluster."""

    node_type: str = NodeType.ECS_SERVICE

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        AssumesEdge,
        ReferencesEdge,
        ProtectedByEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        RoutesToEdge,
    })

    @staticmethod
    def build_urn(
        account_id: str,
        region: str,
        cluster_name: str,
        service_name: str,
    ) -> URN:
        return URN(
            f"urn:aws:ecs:{account_id}:{region}:{cluster_name}/{service_name}",
        )

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        service_name: str,
        task_definition: str | None = None,
        arn: str | None = None,
    ) -> EcsServiceNode:
        meta = NodeMetadata({NK.ECS_SERVICE_NAME: service_name})
        if task_definition is not None:
            meta[NK.ECS_TASK_DEFINITION] = task_definition
        if arn is not None:
            meta[NK.ARN] = arn
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
