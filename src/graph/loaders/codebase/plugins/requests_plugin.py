"""Requests plugin for detecting HTTP egress calls.

Detects usage of ``requests.get()``, ``requests.post()``, etc. and
``requests.Session()`` in function bodies. Tags matching functions
with IO_DIRECTION=egress and IO_TYPE=network.
"""

from __future__ import annotations

import re

from src.graph.graph_models import Node, NodeMetadataKey
from src.graph.loaders.codebase.plugins._base import CodebasePlugin

_REQUESTS_CALL_RE = re.compile(
    r"\brequests\.(get|post|put|patch|delete|head|options)\s*\("
)
_REQUESTS_SESSION_RE = re.compile(r"\brequests\.Session\s*\(")


class RequestsPlugin(CodebasePlugin):
    """Detects requests library HTTP calls (egress/network)."""

    def supported_languages(self) -> set[str]:
        return {"python"}

    def on_function_node(
        self,
        node: Node,
        function_source: str
    ) -> Node:

        if _REQUESTS_CALL_RE.search(function_source) or _REQUESTS_SESSION_RE.search(function_source):
            node.metadata[NodeMetadataKey.IO_DIRECTION] = "egress"
            node.metadata[NodeMetadataKey.IO_TYPE] = "network"

        return node
