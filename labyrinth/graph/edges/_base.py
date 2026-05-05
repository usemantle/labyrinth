"""Shared validation for typed edges."""

from __future__ import annotations

from labyrinth.graph.graph_models import Edge, Node


def validate_edge(
    edge: Edge,
    from_node: Node,
    to_node: Node,
) -> list[str]:
    """Validate that an edge is allowed between two typed nodes.

    Returns a list of violation messages (empty = valid).
    If from_node or to_node is a bare ``Node`` (not subclassed),
    validation is skipped for that side.
    """
    violations: list[str] = []
    edge_cls = type(edge)

    # Skip validation for bare Node instances (backward compat)
    if type(from_node) is Node and type(to_node) is Node:
        return violations

    if type(from_node) is not Node:
        allowed_out = from_node._allowed_outgoing_edges
        if edge_cls not in allowed_out:
            allowed_names = sorted(c.__name__ for c in allowed_out)
            violations.append(
                f"{type(from_node).__name__} does not allow "
                f"outgoing {edge_cls.__name__} edges "
                f"(allowed: {allowed_names})"
            )

    if type(to_node) is not Node:
        allowed_in = to_node._allowed_incoming_edges
        if edge_cls not in allowed_in:
            allowed_names = sorted(c.__name__ for c in allowed_in)
            violations.append(
                f"{type(to_node).__name__} does not allow "
                f"incoming {edge_cls.__name__} edges "
                f"(allowed: {allowed_names})"
            )

    return violations
