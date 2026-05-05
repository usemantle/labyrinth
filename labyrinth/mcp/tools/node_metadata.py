"""MCP tool for updating node metadata."""

from __future__ import annotations

import json

from mcp.server.fastmcp import FastMCP
from labyrinth.mcp.graph_store import GraphStore


def register(mcp: FastMCP, store: GraphStore) -> None:
    @mcp.tool()
    def update_node_metadata(urn: str, metadata: str) -> str:
        """Add or update metadata on a node. Persists to graph.json.

        Args:
            urn: The node URN to update.
            metadata: A JSON string of key-value pairs to merge into
                the node's existing metadata.
        """
        if urn not in store.G:
            return f"Error: node not found in graph: {urn}"

        try:
            kv = json.loads(metadata)
        except (json.JSONDecodeError, TypeError) as exc:
            return f"Error: invalid JSON metadata: {exc}"

        if not isinstance(kv, dict):
            return "Error: metadata must be a JSON object"

        store.update_node_metadata(urn, **kv)
        return f"Updated metadata on {urn}: {kv}"
