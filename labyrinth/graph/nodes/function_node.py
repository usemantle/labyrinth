"""FunctionNode — a function or method discovered in source code."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.calls_edge import CallsEdge
from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.instantiates_edge import InstantiatesEdge
from labyrinth.graph.edges.reads_edge import ReadsEdge
from labyrinth.graph.edges.writes_edge import WritesEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class FunctionNode(Node):
    """A function or method within a source file or class."""

    node_type: str = NodeType.FUNCTION

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        CallsEdge,
        InstantiatesEdge,
        ReadsEdge,
        WritesEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        CallsEdge,
    })

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None,
        *,
        function_name: str,
        start_line: int,
        end_line: int,
        is_method: bool = False,
    ) -> FunctionNode:
        meta = NodeMetadata({
            NK.FUNCTION_NAME: function_name,
            NK.START_LINE: start_line,
            NK.END_LINE: end_line,
            NK.IS_METHOD: is_method,
        })
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
