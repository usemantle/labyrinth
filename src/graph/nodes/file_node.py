"""FileNode — a source file discovered in a codebase."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from src.graph.edges.builds_edge import BuildsEdge
from src.graph.edges.contains_edge import ContainsEdge
from src.graph.edges.depends_on_edge import DependsOnEdge
from src.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class FileNode(Node):
    """A source file within a codebase."""

    node_type: str = NodeType.FILE

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        BuildsEdge,
        ContainsEdge,
        DependsOnEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })

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
