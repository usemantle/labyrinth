"""ColumnNode — a column within a database table."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.references_edge import ReferencesEdge
from labyrinth.graph.edges.soft_reference_edge import SoftReferenceEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class ColumnNode(Node):
    """A column within a database table."""

    node_type: str = NodeType.COLUMN

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        ReferencesEdge,
        SoftReferenceEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        ReferencesEdge,
        SoftReferenceEdge,
    })

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        column_name: str,
        data_type: str | None = None,
        nullable: bool | None = None,
        ordinal_position: int | None = None,
    ) -> ColumnNode:
        meta = NodeMetadata({NK.COLUMN_NAME: column_name})
        if data_type is not None:
            meta[NK.DATA_TYPE] = data_type
        if nullable is not None:
            meta[NK.NULLABLE] = nullable
        if ordinal_position is not None:
            meta[NK.ORDINAL_POSITION] = ordinal_position
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
