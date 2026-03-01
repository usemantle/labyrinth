"""
SQLAlchemy ORM plugin for the codebase loader.

Detects SQLAlchemy model classes by looking for ``__tablename__``
assignments in class bodies and enriches the class node metadata
with ``orm_table`` and ``orm_framework`` fields.
"""

import re

from src.graph.graph_models import Node, NodeMetadataKey
from src.graph.loaders.codebase.plugins._base import CodebasePlugin


_TABLENAME_RE = re.compile(r'__tablename__\s*=\s*["\']([^"\']+)["\']')


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
            node.metadata[NodeMetadataKey.ORM_TABLE] = match.group(1)
            node.metadata[NodeMetadataKey.ORM_FRAMEWORK] = "sqlalchemy"

        return node
