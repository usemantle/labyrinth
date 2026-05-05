"""GroupNode — a collection of Persons in an IdP (e.g. an Okta group)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.okta_edges import (
    OktaAssignedToEdge,
    OktaPartOfEdge,
    OktaPushesToEdge,
)
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class GroupNode(Node):
    """A group of identities sourced from an IdP (Okta, Azure AD, etc.)."""

    node_type: str = NodeType.GROUP

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        OktaAssignedToEdge,
        OktaPushesToEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        OktaPartOfEdge,
    })

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        okta_id: str,
        name: str | None = None,
        description: str | None = None,
    ) -> GroupNode:
        meta = NodeMetadata({NK.GROUP_OKTA_ID: okta_id})
        if name is not None:
            meta[NK.GROUP_NAME] = name
        if description is not None:
            meta[NK.GROUP_DESCRIPTION] = description
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
