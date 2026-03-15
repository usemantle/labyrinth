"""TableNode — a database table or view."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from src.graph.edges.contains_edge import ContainsEdge
from src.graph.edges.models_edge import ModelsEdge
from src.graph.edges.reads_edge import ReadsEdge
from src.graph.edges.writes_edge import WritesEdge
from src.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class TableNode(Node):
    """A table or view within a database schema."""

    node_type: str = NodeType.TABLE

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        ReadsEdge,
        WritesEdge,
        ModelsEdge,
    })

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        table_name: str,
        table_type: str | None = None,
    ) -> TableNode:
        meta = NodeMetadata({NK.TABLE_NAME: table_name})
        if table_type is not None:
            meta[NK.TABLE_TYPE] = table_type
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
