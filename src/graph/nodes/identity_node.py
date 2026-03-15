"""IdentityNode — a principal (database role, IAM role, IdP user, etc.)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from src.graph.edges.reads_edge import ReadsEdge
from src.graph.edges.writes_edge import WritesEdge
from src.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class IdentityNode(Node):
    """A principal identity such as a database role or IAM role."""

    node_type: str = NodeType.IDENTITY

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        ReadsEdge,
        WritesEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset()

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        role_name: str,
        role_login: bool | None = None,
        role_superuser: bool | None = None,
    ) -> IdentityNode:
        meta = NodeMetadata({NK.ROLE_NAME: role_name})
        if role_login is not None:
            meta[NK.ROLE_LOGIN] = role_login
        if role_superuser is not None:
            meta[NK.ROLE_SUPERUSER] = role_superuser
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
