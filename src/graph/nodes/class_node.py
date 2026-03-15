"""ClassNode — a class discovered in source code."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from src.graph.edges.calls_edge import CallsEdge
from src.graph.edges.contains_edge import ContainsEdge
from src.graph.edges.instantiates_edge import InstantiatesEdge
from src.graph.edges.models_edge import ModelsEdge
from src.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class ClassNode(Node):
    """A class definition within a source file."""

    node_type: str = NodeType.CLASS

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        ModelsEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        CallsEdge,
        InstantiatesEdge,
    })

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None,
        *,
        class_name: str,
        start_line: int,
        end_line: int,
        base_classes: str | None = None,
    ) -> ClassNode:
        meta = NodeMetadata({
            NK.CLASS_NAME: class_name,
            NK.START_LINE: start_line,
            NK.END_LINE: end_line,
        })
        if base_classes is not None:
            meta[NK.BASE_CLASSES] = base_classes
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
