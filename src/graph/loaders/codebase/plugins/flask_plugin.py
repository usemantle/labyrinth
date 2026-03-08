"""Flask plugin for detecting HTTP ingress routes.

Detects ``@app.route("/path")``, ``@bp.route("/path", methods=["POST"])``,
etc. and tags matching functions with IO_DIRECTION=ingress, IO_TYPE=network,
plus ROUTE_PATH, API_FRAMEWORK, and optionally HTTP_METHOD metadata.
"""

from __future__ import annotations

import re

from src.graph.graph_models import Node, NodeMetadataKey
from src.graph.loaders.codebase.plugins._base import CodebasePlugin

# Matches @app.route("/path") or @bp.route("/path", methods=["GET", "POST"])
_FLASK_ROUTE_RE = re.compile(
    r"@(\w+)\.route\(\s*[\"']([^\"']*)[\"']"
    r"(?:[^)]*methods\s*=\s*\[([^\]]*)\])?"
)


class FlaskPlugin(CodebasePlugin):
    """Detects Flask route decorators (ingress/network)."""

    def supported_languages(self) -> set[str]:
        return {"python"}

    def on_function_node(
        self,
        node: Node,
        function_source: str
    ) -> Node:

        match = _FLASK_ROUTE_RE.search(function_source)
        if match:
            _router_var, route_path, methods_str = match.groups()
            node.metadata[NodeMetadataKey.IO_DIRECTION] = "ingress"
            node.metadata[NodeMetadataKey.IO_TYPE] = "network"
            node.metadata[NodeMetadataKey.ROUTE_PATH] = route_path
            node.metadata[NodeMetadataKey.API_FRAMEWORK] = "flask"

            if methods_str:
                # Parse methods=["GET", "POST"] → "GET,POST"
                methods = [
                    m.strip().strip("\"'")
                    for m in methods_str.split(",")
                ]
                node.metadata[NodeMetadataKey.HTTP_METHOD] = ",".join(
                    m.upper() for m in methods if m
                )

        return node
