"""BackendGroupNode — a target group / backend service."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from src.graph.edges.contains_edge import ContainsEdge
from src.graph.edges.routes_to_edge import RoutesToEdge
from src.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class BackendGroupNode(Node):
    """A backend group (AWS target group, GCP backend service, etc.)."""

    node_type: str = NodeType.BACKEND_GROUP

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        RoutesToEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        RoutesToEdge,
        ContainsEdge,
    })

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        bg_name: str,
        bg_port: int | None = None,
        bg_protocol: str | None = None,
        bg_target_type: str | None = None,
        bg_health_check: dict | None = None,
        bg_backend_type: str = "aws_target_group",
        arn: str | None = None,
    ) -> BackendGroupNode:
        meta = NodeMetadata({
            NK.BG_NAME: bg_name,
            NK.BG_BACKEND_TYPE: bg_backend_type,
        })
        if bg_port is not None:
            meta[NK.BG_PORT] = bg_port
        if bg_protocol is not None:
            meta[NK.BG_PROTOCOL] = bg_protocol
        if bg_target_type is not None:
            meta[NK.BG_TARGET_TYPE] = bg_target_type
        if bg_health_check is not None:
            meta[NK.BG_HEALTH_CHECK] = bg_health_check
        if arn is not None:
            meta[NK.ARN] = arn
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
