from __future__ import annotations

from pathlib import Path

from mcp.server.fastmcp import FastMCP

from src.mcp.graph_store import GraphStore
from src.mcp.tools import code_data, query, soft_links, traversal


def run_mcp_server(graph_path: Path) -> None:
    """Create a GraphStore and FastMCP server, register all tools, and run
    the server over stdio."""
    store = GraphStore(str(graph_path))
    mcp = FastMCP("Labyrinth Knowledge Graph")

    query.register(mcp, store)
    code_data.register(mcp, store)
    traversal.register(mcp, store)
    soft_links.register(mcp, store)

    mcp.run(transport="stdio")
