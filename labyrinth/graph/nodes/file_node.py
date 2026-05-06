"""FileNode — a source file discovered in a codebase."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.builds_edge import BuildsEdge
from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.depends_on_edge import DependsOnEdge
from labyrinth.graph.edges.executes_edge import ExecutesEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class FileNode(Node):
    """A source file within a codebase."""

    node_type: str = NodeType.FILE

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        BuildsEdge,
        ContainsEdge,
        DependsOnEdge,
        ExecutesEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        ExecutesEdge,
    })

    @staticmethod
    def build_urn(codebase_urn: URN, rel_path: str) -> URN:
        return URN(f"{codebase_urn}/{rel_path}")

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None,
        *,
        file_path: str,
        language: str | None = None,
        size_bytes: int | None = None,
    ) -> FileNode:
        meta = NodeMetadata({NK.FILE_PATH: file_path})
        if language is not None:
            meta[NK.LANGUAGE] = language
        if size_bytes is not None:
            meta[NK.SIZE_BYTES] = size_bytes
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
