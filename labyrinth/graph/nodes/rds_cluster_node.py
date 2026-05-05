"""RdsClusterNode — an RDS cluster or instance."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.hosts_edge import HostsEdge
from labyrinth.graph.edges.protected_by_edge import ProtectedByEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class RdsClusterNode(Node):
    """An AWS RDS cluster or standalone instance."""

    node_type: str = NodeType.RDS_CLUSTER

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        HostsEdge,
        ProtectedByEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })

    @staticmethod
    def build_urn(account_id: str, region: str, cluster_id: str) -> URN:
        return URN(f"urn:aws:rds:{account_id}:{region}:{cluster_id}")

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        cluster_id: str,
        engine: str | None = None,
        endpoint: str | None = None,
        port: int | None = None,
        publicly_accessible: bool | None = None,
        encryption_enabled: bool | None = None,
        multi_az: bool | None = None,
        arn: str | None = None,
    ) -> RdsClusterNode:
        meta = NodeMetadata({NK.RDS_CLUSTER_ID: cluster_id})
        if engine is not None:
            meta[NK.RDS_ENGINE] = engine
        if endpoint is not None:
            meta[NK.RDS_ENDPOINT] = endpoint
        if port is not None:
            meta[NK.RDS_PORT] = port
        if publicly_accessible is not None:
            meta[NK.RDS_PUBLICLY_ACCESSIBLE] = publicly_accessible
        if encryption_enabled is not None:
            meta[NK.RDS_ENCRYPTION_ENABLED] = encryption_enabled
        if multi_az is not None:
            meta[NK.RDS_MULTI_AZ] = multi_az
        if arn is not None:
            meta[NK.ARN] = arn
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
