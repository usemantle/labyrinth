"""CodebaseNode — root of a scanned codebase (filesystem or git repo)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class CodebaseNode(Node):
    """Root node representing a scanned codebase."""

    node_type: str = NodeType.CODEBASE

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset()

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        *,
        repo_name: str,
        file_count: int | None = None,
    ) -> CodebaseNode:
        meta = NodeMetadata({NK.REPO_NAME: repo_name})
        if file_count is not None:
            meta[NK.FILE_COUNT] = file_count
        return cls(
            organization_id=organization_id,
            urn=urn,
            metadata=meta,
        )
