"""
Boto3 S3 plugin for the codebase loader.

Detects direct boto3 S3 client/resource creation and S3 API method calls
in Python code.  Tags code nodes with metadata so a downstream AI agent
can resolve which S3 data nodes they correspond to.

Tier-1 only: no abstraction-layer tracing, no edge creation.
"""

import re

from src.graph.graph_models import Node, NodeMetadataKey
from src.graph.loaders.codebase.plugins._base import CodebasePlugin

# ── Operation sets ────────────────────────────────────────────────────

_WRITE_OPS = frozenset({
    "put_object", "upload_file", "upload_fileobj",
    "copy_object", "create_bucket",
})
_READ_OPS = frozenset({
    "get_object", "download_file", "download_fileobj",
    "head_object", "head_bucket",
    "list_objects_v2", "list_objects",
})
_DELETE_OPS = frozenset({
    "delete_object", "delete_objects", "delete_bucket",
})
_ALL_S3_OPS = _WRITE_OPS | _READ_OPS | _DELETE_OPS

# ── Regex patterns ────────────────────────────────────────────────────

_S3_CLIENT_RE = re.compile(r'\.(?:client|resource)\s*\(\s*["\']s3["\']')

_S3_OPERATION_RE = re.compile(
    r'\.(' + '|'.join(sorted(_ALL_S3_OPS)) + r')\s*\(',
)

_S3_PAGINATOR_RE = re.compile(
    r'\.get_paginator\s*\(\s*["\']list_objects(?:_v2)?["\']',
)


# ── Helpers ───────────────────────────────────────────────────────────

def _classify_operation(op: str) -> str:
    """Return 'read', 'write', or 'delete' for a single S3 operation."""
    if op in _WRITE_OPS:
        return "write"
    if op in _DELETE_OPS:
        return "delete"
    return "read"


def _detect_operations(source: str) -> tuple[str, str] | None:
    """Detect S3 operations in source text.

    Returns:
        A (operations, operation_types) tuple of comma-separated sorted
        strings, or ``None`` if no operations are found.
    """
    ops: set[str] = set()

    for match in _S3_OPERATION_RE.finditer(source):
        ops.add(match.group(1))

    if _S3_PAGINATOR_RE.search(source):
        # get_paginator("list_objects_v2") counts as list_objects_v2
        pag_match = re.search(
            r'\.get_paginator\s*\(\s*["\'](list_objects(?:_v2)?)["\']',
            source,
        )
        if pag_match:
            ops.add(pag_match.group(1))

    if not ops:
        return None

    operations = ",".join(sorted(ops))
    types = ",".join(sorted({_classify_operation(op) for op in ops}))
    return operations, types


# ── Plugin ────────────────────────────────────────────────────────────

NK = NodeMetadataKey


class Boto3S3Plugin(CodebasePlugin):
    """Detects boto3 S3 client creation and S3 API calls in Python code."""

    def supported_languages(self) -> set[str]:
        return {"python"}

    def on_class_node(
        self,
        node: Node,
        class_body_source: str
    ) -> Node:

        if _S3_CLIENT_RE.search(class_body_source):
            node.metadata[NK.AWS_S3_CLIENT] = True

        return node

    def on_function_node(
        self,
        node: Node,
        function_source: str,
    ) -> Node:
        if _S3_CLIENT_RE.search(function_source):
            node.metadata[NK.AWS_S3_CLIENT] = True

        result = _detect_operations(function_source)
        if result:
            node.metadata[NK.AWS_S3_OPERATIONS] = result[0]
            node.metadata[NK.AWS_S3_OPERATION_TYPE] = result[1]

        return node
