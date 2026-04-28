"""PersonNode — a human identity in an IdP (e.g. an Okta user)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from src.graph.edges.contains_edge import ContainsEdge
from src.graph.edges.okta_edges import (
    OktaAssignedToEdge,
    OktaMapsToEdge,
    OktaPartOfEdge,
)
from src.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class PersonNode(Node):
    """A human identity sourced from an IdP (Okta, Azure AD, etc.)."""

    node_type: str = NodeType.PERSON

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        OktaPartOfEdge,
        OktaAssignedToEdge,
        OktaMapsToEdge,
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
        okta_id: str,
        email: str | None = None,
        login: str | None = None,
        status: str | None = None,
        display_name: str | None = None,
    ) -> PersonNode:
        meta = NodeMetadata({NK.PERSON_OKTA_ID: okta_id})
        if email is not None:
            meta[NK.PERSON_EMAIL] = email
        if login is not None:
            meta[NK.PERSON_LOGIN] = login
        if status is not None:
            meta[NK.PERSON_STATUS] = status
        if display_name is not None:
            meta[NK.PERSON_DISPLAY_NAME] = display_name
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
