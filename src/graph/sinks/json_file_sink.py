from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.graph.graph_models import Edge, Node, NodeMetadataKey, NodeType
from src.graph.sinks.sink import Sink

logger = logging.getLogger(__name__)

NK = NodeMetadataKey


def classify_node(node: Node) -> str:
    """Return a node type string based on metadata."""
    m = node.metadata
    if NK.FUNCTION_NAME in m:
        return NodeType.FUNCTION
    if NK.CLASS_NAME in m:
        return NodeType.CLASS
    if NK.COLUMN_NAME in m:
        return NodeType.COLUMN
    if NK.TABLE_NAME in m:
        return NodeType.TABLE
    if NK.SCHEMA_NAME in m:
        return NodeType.SCHEMA
    if NK.REPO_NAME in m:
        return NodeType.CODEBASE
    if NK.FILE_PATH in m and NK.CLASS_NAME not in m and NK.FUNCTION_NAME not in m:
        return NodeType.FILE
    if NK.DATABASE_NAME in m:
        return NodeType.DATABASE
    if NK.PATH_PATTERN in m:
        if NK.PARTITION_TYPE in m:
            return NodeType.S3_PARTITION
        if NK.OBJECT_COUNT not in m:
            return NodeType.S3_PREFIX
        return NodeType.S3_OBJECT
    if NK.BUCKET_NAME in m:
        return NodeType.S3_BUCKET
    if NK.PACKAGE_NAME in m:
        return NodeType.DEPENDENCY
    if NK.ROLE_NAME in m:
        return NodeType.DB_ROLE
    return NodeType.UNKNOWN


def _serialize_node(node: Node) -> dict:
    node_type = node.node_type if node.node_type != NodeType.UNKNOWN else classify_node(node)
    return {
        "urn": str(node.urn),
        "organization_id": str(node.organization_id),
        "parent_urn": str(node.parent_urn) if node.parent_urn else None,
        "node_type": node_type,
        "metadata": dict(node.metadata.items()),
    }


def _serialize_edge(edge: Edge) -> dict:
    edge_type = edge.edge_type
    return {
        "uuid": str(edge.uuid),
        "organization_id": str(edge.organization_id),
        "from_urn": str(edge.from_urn),
        "to_urn": str(edge.to_urn),
        "edge_type": edge_type,
        "metadata": dict(edge.metadata.items()),
    }


class JsonFileSink(Sink):
    """Write graph data to a JSON file."""

    def __init__(self, output_path: Path):
        self._output_path = output_path

    def _read(self) -> dict:
        """Read the current graph JSON from disk."""
        if not self._output_path.exists():
            return {"nodes": [], "edges": [], "soft_links": []}
        with open(self._output_path) as f:
            return json.load(f)

    def _atomic_write(self, data: dict) -> None:
        """Write data to disk atomically via a temp file rename."""
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = str(self._output_path) + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        os.replace(tmp_path, str(self._output_path))

    def write(self, nodes: list[Node], edges: list[Edge]) -> None:
        data = {
            "generated_at": datetime.now(UTC).isoformat(),
            "node_count": len(nodes),
            "edge_count": len(edges),
            "nodes": [_serialize_node(n) for n in nodes],
            "edges": [_serialize_edge(e) for e in edges],
            "soft_links": [],
        }
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        self._output_path.write_text(json.dumps(data, indent=2, default=str))
        logger.info("Graph JSON saved to %s", self._output_path)

    def update_node_metadata(self, urn: str, **kwargs: Any) -> None:
        data = self._read()
        for node in data["nodes"]:
            if node["urn"] == urn:
                node.setdefault("metadata", {}).update(kwargs)
                break
        self._atomic_write(data)

    def delete_node_metadata(self, urn: str, *keys: str) -> None:
        data = self._read()
        for node in data["nodes"]:
            if node["urn"] == urn:
                meta = node.get("metadata", {})
                for k in keys:
                    meta.pop(k, None)
                break
        self._atomic_write(data)

    def add_soft_link(self, link: dict) -> None:
        data = self._read()
        data.setdefault("soft_links", []).append(link)
        self._atomic_write(data)

    def remove_soft_link(self, link_id: str) -> None:
        data = self._read()
        data["soft_links"] = [
            sl for sl in data.get("soft_links", []) if sl.get("id") != link_id
        ]
        self._atomic_write(data)
