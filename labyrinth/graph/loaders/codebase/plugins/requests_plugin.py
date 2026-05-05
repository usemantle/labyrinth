"""Requests plugin for detecting HTTP egress calls.

Detects usage of ``requests.get()``, ``requests.post()``, etc. and
``requests.Session()`` in function bodies. Tags matching functions
with IO_DIRECTION=egress and IO_TYPE=network.

All enrichment is import-gated: only files that import ``requests``
are considered.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from labyrinth.graph.graph_models import Node, NodeMetadataKey
from labyrinth.graph.loaders.codebase.plugins._base import CodebasePlugin

if TYPE_CHECKING:
    from labyrinth.graph.graph_models import Edge
    from labyrinth.graph.loaders.codebase.codebase_loader import PostProcessContext

_REQUESTS_CALL_RE = re.compile(
    r"\brequests\.(get|post|put|patch|delete|head|options)\s*\("
)
_REQUESTS_SESSION_RE = re.compile(r"\brequests\.Session\s*\(")


class RequestsPlugin(CodebasePlugin):
    """Detects requests library HTTP calls (egress/network)."""

    @classmethod
    def auto_detect(cls, root_path):
        return cls._dependency_mentions(root_path, "requests")

    def supported_languages(self) -> set[str]:
        return {"python"}

    def post_process(
        self,
        nodes: list[Node],
        edges: list[Edge],
        context: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        """Detect requests library calls on function nodes."""
        for node in nodes:
            if NodeMetadataKey.FUNCTION_NAME not in node.metadata:
                continue
            rel_path = node.metadata.get(NodeMetadataKey.FILE_PATH)
            if not rel_path:
                continue
            file_source = context.file_sources.get(rel_path)
            if not file_source or not self._file_imports_library(file_source, "requests"):
                continue

            func_source = self._get_node_source(node, context)
            if not func_source:
                continue

            if _REQUESTS_CALL_RE.search(func_source) or _REQUESTS_SESSION_RE.search(func_source):
                node.metadata[NodeMetadataKey.IO_DIRECTION] = "egress"
                node.metadata[NodeMetadataKey.IO_TYPE] = "network"

        return nodes, edges
