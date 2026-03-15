"""Flask plugin for detecting HTTP ingress routes.

Detects ``@app.route("/path")``, ``@bp.route("/path", methods=["POST"])``,
etc. and tags matching functions with IO_DIRECTION=ingress, IO_TYPE=network,
plus ROUTE_PATH, API_FRAMEWORK, and optionally HTTP_METHOD metadata.

All enrichment is import-gated: only files that import from ``flask``
are considered.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from src.graph.graph_models import Node, NodeMetadataKey
from src.graph.loaders.codebase.plugins._base import CodebasePlugin

if TYPE_CHECKING:
    from src.graph.graph_models import Edge
    from src.graph.loaders.codebase.codebase_loader import PostProcessContext

# Matches @app.route("/path") or @bp.route("/path", methods=["GET", "POST"])
_FLASK_ROUTE_RE = re.compile(
    r"@(\w+)\.route\(\s*[\"']([^\"']*)[\"']"
    r"(?:[^)]*methods\s*=\s*\[([^\]]*)\])?"
)

NK = NodeMetadataKey


class FlaskPlugin(CodebasePlugin):
    """Detects Flask route decorators (ingress/network)."""

    @classmethod
    def auto_detect(cls, root_path):
        return cls._dependency_mentions(root_path, "flask")

    def supported_languages(self) -> set[str]:
        return {"python"}

    def post_process(
        self,
        nodes: list[Node],
        edges: list[Edge],
        context: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        """Detect Flask route decorators on function nodes."""
        for node in nodes:
            if NK.FUNCTION_NAME not in node.metadata:
                continue
            rel_path = node.metadata.get(NK.FILE_PATH)
            if not rel_path:
                continue
            file_source = context.file_sources.get(rel_path)
            if not file_source or not self._file_imports_library(file_source, "flask"):
                continue

            func_source = self._get_node_source(node, context)
            if not func_source:
                continue

            # Include decorator lines above the function
            start_line = node.metadata.get(NK.START_LINE)
            if start_line is not None and start_line > 0:
                lines = file_source.splitlines()
                deco_start = max(0, start_line - 10)
                prefix_lines = lines[deco_start:start_line]
                deco_lines = []
                for line in reversed(prefix_lines):
                    stripped = line.strip()
                    if stripped.startswith("@") or (deco_lines and stripped.endswith(")")):
                        deco_lines.insert(0, line)
                    elif stripped == "":
                        continue
                    else:
                        break
                full_source = "\n".join(deco_lines) + "\n" + func_source if deco_lines else func_source
            else:
                full_source = func_source

            match = _FLASK_ROUTE_RE.search(full_source)
            if match:
                _router_var, route_path, methods_str = match.groups()
                node.metadata[NK.IO_DIRECTION] = "ingress"
                node.metadata[NK.IO_TYPE] = "network"
                node.metadata[NK.ROUTE_PATH] = route_path
                node.metadata[NK.API_FRAMEWORK] = "flask"

                if methods_str:
                    methods = [
                        m.strip().strip("\"'")
                        for m in methods_str.split(",")
                    ]
                    node.metadata[NK.HTTP_METHOD] = ",".join(
                        m.upper() for m in methods if m
                    )

        return nodes, edges
