"""Rust stdlib ingress/egress detection (placeholder).

This module will be implemented in a future iteration to detect
Rust stdlib patterns like std::net, std::env, std::process, etc.
"""

from __future__ import annotations

from labyrinth.graph.graph_models import Node


def enrich_stdlib_io(
    nodes: list[Node],
    file_sources: dict[str, str],
) -> list[Node]:
    """Placeholder — returns nodes unchanged."""
    return nodes
