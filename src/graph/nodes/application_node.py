"""ApplicationNode — a downstream application registered in an IdP (e.g. an Okta app)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from src.graph.edges.contains_edge import ContainsEdge
from src.graph.edges.idp_assigned_to_edge import IdpAssignedToEdge
from src.graph.edges.idp_pushes_to_edge import IdpPushesToEdge
from src.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class ApplicationNode(Node):
    """An application registered in an IdP (Okta, Azure AD, etc.)."""

    node_type: str = NodeType.APPLICATION

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset()
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        IdpAssignedToEdge,
        IdpPushesToEdge,
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
        label: str | None = None,
        sign_on_mode: str | None = None,
        status: str | None = None,
    ) -> ApplicationNode:
        meta = NodeMetadata({NK.APP_OKTA_ID: okta_id})
        if name is not None:
            meta[NK.APP_NAME] = name
        if label is not None:
            meta[NK.APP_LABEL] = label
        if sign_on_mode is not None:
            meta[NK.APP_SIGN_ON_MODE] = sign_on_mode
        if status is not None:
            meta[NK.APP_STATUS] = status
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
