"""
SQLAlchemy ORM plugin for the codebase loader.

Detects SQLAlchemy model classes by looking for ``__tablename__``
assignments in class bodies and enriches the class node metadata
with ``orm_table`` and ``orm_framework`` fields.

Also detects SQLAlchemy session operations in function bodies and
classifies them as read, write, or delete.

The ``post_process`` hook links functions to ORM classes they reference
via CODE_TO_CODE edges.

All enrichment is import-gated: only files that import from
``sqlalchemy`` are considered.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from labyrinth.graph.edges.calls_edge import CallsEdge
from labyrinth.graph.graph_models import (
    Edge,
    EdgeMetadata,
    EdgeMetadataKey,
    Node,
    NodeMetadataKey,
)
from labyrinth.graph.loaders.codebase.plugins._base import CodebasePlugin

if TYPE_CHECKING:
    from labyrinth.graph.loaders.codebase.codebase_loader import PostProcessContext


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

    @classmethod
    def auto_detect(cls, root_path):
        return cls._dependency_mentions(root_path, "sqlalchemy")

    def supported_languages(self) -> set[str]:
        return {"python"}

    def post_process(
        self,
        nodes: list[Node],
        edges: list[Edge],
        context: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        """Enrich class/function nodes and link functions to ORM classes."""
        # Step 1: Enrich class nodes with __tablename__ metadata
        for node in nodes:
            if NK.CLASS_NAME not in node.metadata:
                continue
            rel_path = node.metadata.get(NK.FILE_PATH)
            if not rel_path:
                continue
            file_source = context.file_sources.get(rel_path)
            if not file_source or not self._file_imports_library(file_source, "sqlalchemy"):
                continue

            class_source = self._get_node_source(node, context)
            if not class_source:
                continue

            match = _TABLENAME_RE.search(class_source)
            if match:
                node.metadata[NK.ORM_TABLE] = match.group(1)
                node.metadata[NK.ORM_FRAMEWORK] = "sqlalchemy"

        # Step 2: Enrich function nodes with ORM operation metadata
        for node in nodes:
            if NK.FUNCTION_NAME not in node.metadata:
                continue
            rel_path = node.metadata.get(NK.FILE_PATH)
            if not rel_path:
                continue
            file_source = context.file_sources.get(rel_path)
            if not file_source or not self._file_imports_library(file_source, "sqlalchemy"):
                continue

            func_source = self._get_node_source(node, context)
            if not func_source:
                continue

            ops: set[str] = set()
            for match in _ORM_OP_RE.finditer(func_source):
                ops.add(match.group(1))

            if _FILTER_DELETE_RE.search(func_source):
                ops.add("delete")

            if not ops:
                continue

            node.metadata[NK.ORM_OPERATIONS] = ",".join(sorted(ops))
            node.metadata[NK.ORM_OPERATION_TYPE] = ",".join(
                sorted({_classify_operation(op) for op in ops})
            )
            node.metadata[NK.ORM_FRAMEWORK] = "sqlalchemy"

        # Step 3: Link functions to ORM classes they reference
        orm_classes: dict[str, Node] = {}
        for node in nodes:
            if NK.ORM_TABLE in node.metadata and NK.CLASS_NAME in node.metadata:
                orm_classes[node.metadata[NK.CLASS_NAME]] = node

        if not orm_classes:
            return nodes, edges

        class_patterns = {
            name: re.compile(r"\b" + re.escape(name) + r"\b")
            for name in orm_classes
        }

        for node in nodes:
            if NK.ORM_OPERATIONS not in node.metadata:
                continue
            if NK.FUNCTION_NAME not in node.metadata:
                continue

            func_source = self._get_node_source(node, context)
            if not func_source:
                continue

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
                edge = CallsEdge.create(
                    context.organization_id,
                    node.urn,
                    orm_node.urn,
                    metadata=EdgeMetadata({
                        EK.DETECTION_METHOD: "orm_model_reference",
                        EK.CONFIDENCE: 0.9,
                        EK.ORM_FRAMEWORK: "sqlalchemy",
                        EK.ORM_CLASS: class_name,
                    }),
                )
                edges.append(edge)

        return nodes, edges
