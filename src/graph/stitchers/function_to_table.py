"""Stitcher: FUNCTION nodes -> TABLE nodes via ReadsEdge."""

from __future__ import annotations

import uuid
from pathlib import Path

from src.graph.edges.reads_edge import ReadsEdge
from src.graph.graph_models import (
    URN,
    EdgeMetadata,
    EdgeMetadataKey,
    Graph,
    NodeMetadata,
    NodeMetadataKey,
)
from src.graph.stitchers._base import Stitcher


class FunctionToTableStitcher(Stitcher):
    """Link functions that reference ORM class names to the corresponding tables."""

    def stitch(self, organization_id: uuid.UUID, graph: Graph, context: dict) -> Graph:
        NK = NodeMetadataKey
        result = Graph()
        code_base_paths: list[str] = context.get("code_base_paths", [])

        idx = self.index_nodes(graph, metadata_keys={NK.TABLE_NAME})
        table_registry = idx.lookup(NK.TABLE_NAME)
        if not table_registry:
            return result

        # Build ORM registry
        orm_registry: dict[str, tuple[URN, str]] = {}
        for node in graph.nodes:
            if NK.ORM_TABLE in node.metadata:
                orm_registry[node.metadata[NK.CLASS_NAME]] = (
                    node.urn,
                    node.metadata[NK.ORM_TABLE],
                )

        if not orm_registry:
            return result

        for node in graph.nodes:
            if NK.FUNCTION_NAME not in node.metadata:
                continue

            source_text = _read_function_source(code_base_paths, node.metadata)
            if not source_text:
                continue

            for class_name, (_, table_name) in orm_registry.items():
                if class_name in source_text and table_name in table_registry:
                    table_urn = table_registry[table_name]
                    result.edges.append(ReadsEdge.create(
                        organization_id,
                        node.urn,
                        table_urn,
                        metadata=EdgeMetadata({
                            EdgeMetadataKey.DETECTION_METHOD: "orm_reference",
                            EdgeMetadataKey.CONFIDENCE: 0.9,
                            EdgeMetadataKey.REFERENCED_MODEL: class_name,
                            EdgeMetadataKey.TABLE_NAME: table_name,
                        }),
                    ))

        return result


def _read_function_source(base_paths: list[str], metadata: NodeMetadata) -> str:
    """Read the source text of a function from disk."""
    NK = NodeMetadataKey
    rel_path = metadata.get(NK.FILE_PATH, "")
    start = metadata.get(NK.START_LINE)
    end = metadata.get(NK.END_LINE)

    if not rel_path or start is None or end is None:
        return ""

    for base in base_paths:
        full_path = Path(base) / rel_path
        if full_path.is_file():
            try:
                lines = full_path.read_text(encoding="utf-8", errors="replace").splitlines()
                return "\n".join(lines[start:end + 1])
            except OSError:
                continue

    return ""
