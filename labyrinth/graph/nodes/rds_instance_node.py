"""RdsInstanceNode — an AWS RDS DB instance."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.hosts_edge import HostsEdge
from labyrinth.graph.edges.protected_by_edge import ProtectedByEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType
from labyrinth.graph.nodes._aws_resource_mixin import AwsResourceMixin

NK = NodeMetadataKey


@dataclass
class RdsInstanceNode(AwsResourceMixin, Node):
    """An AWS RDS DB instance."""

    node_type: str = NodeType.RDS_INSTANCE

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        HostsEdge,
        ProtectedByEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })

    @classmethod
    def build_urn(cls, account_id: str, region: str, instance_id: str) -> URN:
        return cls.urn_from_arn(
            f"arn:aws:rds:{region}:{account_id}:db:{instance_id}",
        )

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        instance_id: str,
        engine: str | None = None,
        endpoint: str | None = None,
        port: int | None = None,
        publicly_accessible: bool | None = None,
        encryption_enabled: bool | None = None,
        multi_az: bool | None = None,
        arn: str | None = None,
    ) -> RdsInstanceNode:
        meta = NodeMetadata({NK.RDS_INSTANCE_ID: instance_id})
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
