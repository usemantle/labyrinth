"""
SQLAlchemy ORM plugin for the codebase loader.

Detects SQLAlchemy model classes by looking for ``__tablename__``
assignments in class bodies and enriches the class node metadata
with ``orm_table`` and ``orm_framework`` fields.

Also detects SQLAlchemy session operations in function bodies and
classifies them as read, write, or delete.

The ``post_process`` hook links functions to ORM classes they reference
via CODE_TO_CODE edges.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from src.graph.graph_models import (
    Edge,
    EdgeMetadata,
    EdgeMetadataKey,
    Node,
    NodeMetadataKey,
    RelationType,
)
from src.graph.loaders._helpers import make_edge
from src.graph.loaders.codebase.plugins._base import CodebasePlugin

if TYPE_CHECKING:
    from src.graph.loaders.codebase.codebase_loader import PostProcessContext


_TABLENAME_RE = re.compile(r'__tablename__\s*=\s*["\']([^"\']+)["\']')

# ── Operation classification ──────────────────────────────────────────

_READ_OPS = frozenset({"query", "execute", "get", "scalar", "scalars"})
_WRITE_OPS = frozenset({"add", "add_all", "merge", "bulk_save_objects", "flush", "commit"})
_DELETE_OPS = frozenset({"delete"})
_ALL_OPS = _READ_OPS | _WRITE_OPS | _DELETE_OPS

_ORM_OP_RE = re.compile(
    r'\.(' + '|'.join(sorted(_ALL_OPS)) + r')\s*\(',
)

# .filter(...).delete() — bulk delete via query
_FILTER_DELETE_RE = re.compile(r'\.filter\s*\([^)]*\)\s*\.delete\s*\(')

NK = NodeMetadataKey


def _classify_operation(op: str) -> str:
    """Return 'read', 'write', or 'delete' for a single ORM operation."""
    if op in _WRITE_OPS:
        return "write"
    if op in _DELETE_OPS:
        return "delete"
    return "read"


class SQLAlchemyPlugin(CodebasePlugin):
    """Detects SQLAlchemy ``__tablename__`` and enriches class metadata."""

    def on_class_node(
        self,
        node: Node,
        class_body_source: str,
        language: str,
    ) -> Node:
        if language != "python":
            return node

        match = _TABLENAME_RE.search(class_body_source)
        if match:
            node.metadata[NK.ORM_TABLE] = match.group(1)
            node.metadata[NK.ORM_FRAMEWORK] = "sqlalchemy"

        return node

    def on_function_node(
        self,
        node: Node,
        function_source: str,
        language: str,
    ) -> Node:
        if language != "python":
            return node

        ops: set[str] = set()
        for match in _ORM_OP_RE.finditer(function_source):
            ops.add(match.group(1))

        if _FILTER_DELETE_RE.search(function_source):
            ops.add("delete")

        if not ops:
            return node

        node.metadata[NK.ORM_OPERATIONS] = ",".join(sorted(ops))
        node.metadata[NK.ORM_OPERATION_TYPE] = ",".join(
            sorted({_classify_operation(op) for op in ops})
        )
        node.metadata[NK.ORM_FRAMEWORK] = "sqlalchemy"

        return node

    def post_process(
        self,
        nodes: list[Node],
        edges: list[Edge],
        context: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        """Link functions to ORM classes they reference via CODE_TO_CODE edges."""
        # Step 1: Build ORM class registry {class_name: class_urn}
        orm_classes: dict[str, Node] = {}
        for node in nodes:
            if NK.ORM_TABLE in node.metadata and NK.CLASS_NAME in node.metadata:
                orm_classes[node.metadata[NK.CLASS_NAME]] = node

        if not orm_classes:
            return nodes, edges

        # Pre-compile word-boundary patterns for each ORM class name
        class_patterns = {
            name: re.compile(r"\b" + re.escape(name) + r"\b")
            for name in orm_classes
        }

        # Step 2: For each function with ORM_OPERATIONS, check for class references
        for node in nodes:
            if NK.ORM_OPERATIONS not in node.metadata:
                continue
            if NK.FUNCTION_NAME not in node.metadata:
                continue

            # Get function source from context
            file_path = node.metadata.get(NK.FILE_PATH)
            start_line = node.metadata.get(NK.START_LINE)
            end_line = node.metadata.get(NK.END_LINE)
            if not file_path or start_line is None or end_line is None:
                continue

            source = context.file_sources.get(file_path)
            if not source:
                continue

            lines = source.splitlines()
            func_source = "\n".join(lines[start_line:end_line + 1])

            # Check each ORM class name against function source
            referenced: list[str] = []
            for class_name, pattern in class_patterns.items():
                if pattern.search(func_source):
                    referenced.append(class_name)

            if not referenced:
                continue

            referenced.sort()
            node.metadata[NK.ORM_MODELS] = ",".join(referenced)

            for class_name in referenced:
                orm_node = orm_classes[class_name]
                EK = EdgeMetadataKey
                edge = make_edge(
                    context.organization_id,
                    node.urn,
                    orm_node.urn,
                    RelationType.CODE_TO_CODE,
                    EdgeMetadata({
                        EK.DETECTION_METHOD: "orm_model_reference",
                        EK.CONFIDENCE: 0.9,
                        EK.ORM_FRAMEWORK: "sqlalchemy",
                        EK.ORM_CLASS: class_name,
                    }),
                )
                edges.append(edge)

        return nodes, edges
