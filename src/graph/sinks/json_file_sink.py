from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from src.graph.graph_models import Edge, Node, NodeMetadataKey
from src.graph.sinks.sink import Sink

logger = logging.getLogger(__name__)

NK = NodeMetadataKey


def classify_node(node: Node) -> str:
    """Return a node type string based on metadata."""
    m = node.metadata
    if NK.FUNCTION_NAME in m:
        return "function"
    if NK.CLASS_NAME in m:
        return "class"
    if NK.COLUMN_NAME in m:
        return "column"
    if NK.TABLE_NAME in m:
        return "table"
    if NK.SCHEMA_NAME in m:
        return "schema"
    if NK.REPO_NAME in m:
        return "codebase"
    if NK.FILE_PATH in m and NK.CLASS_NAME not in m and NK.FUNCTION_NAME not in m:
        return "file"
    if NK.DATABASE_NAME in m:
        return "database"
    if NK.PATH_PATTERN in m:
        if NK.PARTITION_TYPE in m:
            return "s3_partition"
        if NK.OBJECT_COUNT not in m:
            return "s3_prefix"
        return "s3_object"
    if NK.BUCKET_NAME in m:
        return "s3_bucket"
    return "unknown"


def _serialize_node(node: Node) -> dict:
    return {
        "urn": str(node.urn),
        "organization_id": str(node.organization_id),
        "parent_urn": str(node.parent_urn) if node.parent_urn else None,
        "node_type": classify_node(node),
        "metadata": dict(node.metadata.items()),
    }


def _serialize_edge(edge: Edge) -> dict:
    return {
        "uuid": str(edge.uuid),
        "organization_id": str(edge.organization_id),
        "from_urn": str(edge.from_urn),
        "to_urn": str(edge.to_urn),
        "relation_type": edge.relation_type.value,
        "metadata": dict(edge.metadata.items()),
    }


class JsonFileSink(Sink):
    """Write graph data to a JSON file."""

    def __init__(self, output_path: Path):
        self._output_path = output_path

    def write(self, nodes: list[Node], edges: list[Edge]) -> None:
        data = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "node_count": len(nodes),
            "edge_count": len(edges),
            "nodes": [_serialize_node(n) for n in nodes],
            "edges": [_serialize_edge(e) for e in edges],
        }
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        self._output_path.write_text(json.dumps(data, indent=2, default=str))
        logger.info("Graph JSON saved to %s", self._output_path)
