from __future__ import annotations

import networkx as nx


def _node_label(node: dict) -> str:
    """Short display label for a node."""
    m = node.get("metadata", {})
    for key in ("function_name", "class_name", "column_name", "table_name",
                "schema_name", "repo_name", "database_name"):
        if key in m:
            return str(m[key])
    if "file_path" in m:
        return m["file_path"].rsplit("/", 1)[-1]
    return node["urn"].rsplit("/", 1)[-1]


def _format_node(node: dict, compact: bool = False) -> str:
    """Format a node as a readable string."""
    label = _node_label(node)
    ntype = node.get("node_type", "unknown")
    if compact:
        return f"[{ntype}] {label}  ({node['urn']})"
    lines = [
        f"[{ntype}] {label}",
        f"  URN: {node['urn']}",
    ]
    if node.get("parent_urn"):
        lines.append(f"  Parent: {node['parent_urn']}")
    for k, v in node.get("metadata", {}).items():
        lines.append(f"  {k}: {v}")
    return "\n".join(lines)


def _format_edge_data(from_urn: str, to_urn: str, data: dict) -> str:
    """Format an edge as a readable string."""
    parts = [f"{data.get('relation_type', '?')}: {from_urn} → {to_urn}"]
    for k, v in data.get("metadata", {}).items():
        parts.append(f"  {k}: {v}")
    return "\n".join(parts)


def _lookup_edge_label(G: nx.MultiDiGraph, urn_a: str, urn_b: str) -> str:
    """Find the relation type between two adjacent nodes and return a
    formatted arrow showing direction."""
    if G.has_edge(urn_a, urn_b):
        for _key, data in G[urn_a][urn_b].items():
            return f"  --[{data.get('relation_type', '?')}]-->"
    if G.has_edge(urn_b, urn_a):
        for _key, data in G[urn_b][urn_a].items():
            return f"  <--[{data.get('relation_type', '?')}]--"
    return "  --[?]-->"
