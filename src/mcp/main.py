from __future__ import annotations

import functools
from pathlib import Path

from mcp.server.fastmcp import FastMCP
from src.mcp.graph_store import GraphStore
from src.mcp.tools import code_data, query, security, soft_links, traversal


class _LockedMCP:
    """Wrapper around FastMCP that auto-wraps tool functions with a lock.

    Every tool registered via ``@mcp.tool()`` will acquire ``lock``
    for the duration of the call, ensuring that reads cannot race with
    a background graph reload.
    """

    def __init__(self, mcp: FastMCP, lock):
        self._mcp = mcp
        self._lock = lock

    def tool(self, *args, **kwargs):
        real_decorator = self._mcp.tool(*args, **kwargs)

        def wrapper(fn):
            @functools.wraps(fn)
            def locked(*a, **kw):
                with self._lock:
                    return fn(*a, **kw)
            return real_decorator(locked)

        return wrapper

    def __getattr__(self, name):
        return getattr(self._mcp, name)


def run_mcp_server(graph_path: Path) -> None:
    """Create a GraphStore and FastMCP server, register all tools, and run
    the server over stdio."""
    store = GraphStore(str(graph_path))
    mcp = FastMCP("Labyrinth Knowledge Graph")
    locked_mcp = _LockedMCP(mcp, store.lock)

    query.register(locked_mcp, store)
    code_data.register(locked_mcp, store)
    traversal.register(locked_mcp, store)
    soft_links.register(locked_mcp, store)
    security.register(locked_mcp, store)

    mcp.run(transport="stdio")
