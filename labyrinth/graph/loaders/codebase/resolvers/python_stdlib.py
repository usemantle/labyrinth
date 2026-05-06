"""Python stdlib ingress/egress detection.

Walks function nodes and matches regex patterns against stdlib usage
to tag functions with IO_DIRECTION and IO_TYPE metadata.
"""

from __future__ import annotations

import re

from labyrinth.graph.graph_models import Node, NodeMetadataKey

# (compiled_regex, io_direction, io_type)
_PATTERNS: list[tuple[re.Pattern[str], str, str]] = [
    # Environment variable access
    (re.compile(r"\bos\.environ\b"), "ingress", "env"),
    (re.compile(r"\bos\.getenv\s*\("), "ingress", "env"),
    # CLI arguments
    (re.compile(r"\bsys\.argv\b"), "ingress", "cli"),
    # stdin
    (re.compile(r"\bsys\.stdin\b"), "ingress", "file"),
    # Subprocess (egress)
    (re.compile(r"\bsubprocess\.(run|Popen|call)\s*\("), "egress", "subprocess"),
    # argparse (ingress/cli)
    (re.compile(r"\bArgumentParser\s*\("), "ingress", "cli"),
    (re.compile(r"\.add_argument\s*\("), "ingress", "cli"),
    (re.compile(r"\.parse_args\s*\("), "ingress", "cli"),
    # Socket server (ingress/network)
    (re.compile(r"\bsocket\.bind\s*\(|\.bind\s*\("), "ingress", "network"),
    (re.compile(r"\bsocket\.listen\s*\(|\.listen\s*\("), "ingress", "network"),
    # Socket client (egress/network)
    (re.compile(r"\.connect\s*\("), "egress", "network"),
]


def enrich_stdlib_io(
    nodes: list[Node],
    file_sources: dict[str, str],
) -> list[Node]:
    """Tag function nodes with IO_DIRECTION/IO_TYPE based on stdlib usage.

    Reads each function's source from file_sources using line numbers,
    then matches against known stdlib patterns.

    Args:
        nodes: All graph nodes (only function nodes are inspected).
        file_sources: Mapping of rel_path -> full file source text.

    Returns:
        The same nodes list (mutated in place for matching functions).
    """
    for node in nodes:
        if NodeMetadataKey.FUNCTION_NAME not in node.metadata:
            continue
        rel_path = node.metadata.get(NodeMetadataKey.FILE_PATH)
        if not rel_path or rel_path not in file_sources:
            continue

        func_source = _get_function_source(node, file_sources[rel_path])
        if not func_source:
            continue

        for pattern, direction, io_type in _PATTERNS:
            if pattern.search(func_source):
                node.metadata[NodeMetadataKey.IO_DIRECTION] = direction
                node.metadata[NodeMetadataKey.IO_TYPE] = io_type
                break  # first match wins

    return nodes


def _get_function_source(node: Node, file_source: str) -> str | None:
    """Extract a function's source text from its file using line numbers."""
    start = node.metadata.get(NodeMetadataKey.START_LINE)
    end = node.metadata.get(NodeMetadataKey.END_LINE)
    if start is None or end is None:
        return None
    lines = file_source.splitlines()
    return "\n".join(lines[start:end + 1])
