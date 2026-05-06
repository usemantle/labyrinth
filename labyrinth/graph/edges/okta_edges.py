"""Okta-sourced edges (okta:assigned_to, okta:maps_to, okta:part_of, okta:pushes_to).

All four share the same constructor logic — only the edge_type differs.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from labyrinth.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata, EdgeType


@dataclass
class _OktaEdge(Edge):
    """Base for namespaced Okta edges. Subclasses set the edge_type field default."""

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        metadata: EdgeMetadata | None = None,
    ) -> _OktaEdge:
        return cls(
            uuid=uuid.uuid5(EDGE_NAMESPACE, f"{from_urn}:{to_urn}:{cls.edge_type}"),
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=metadata or EdgeMetadata(),
        )


@dataclass
class OktaAssignedToEdge(_OktaEdge):
    edge_type: str = EdgeType.OKTA_ASSIGNED_TO


@dataclass
class OktaMapsToEdge(_OktaEdge):
    edge_type: str = EdgeType.OKTA_MAPS_TO


@dataclass
class OktaPartOfEdge(_OktaEdge):
    edge_type: str = EdgeType.OKTA_PART_OF


@dataclass
class OktaPushesToEdge(_OktaEdge):
    edge_type: str = EdgeType.OKTA_PUSHES_TO
