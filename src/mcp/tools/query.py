from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from src.mcp._formatting import _format_node, _node_label
from src.mcp.graph_store import GraphStore


def register(mcp: FastMCP, store: GraphStore) -> None:
    @mcp.tool()
    def get_graph_summary() -> str:
        """Get overview statistics of the knowledge graph including node counts
        by type (database, schema, table, column, codebase, file, class,
        function), edge counts by relation type, and generation timestamp."""
        node_counts = {
            ntype: len(urns) for ntype, urns in sorted(store.nodes_by_type.items())
        }
        edge_counts = {
            etype: len(triples) for etype, triples in sorted(store.edges_by_type.items())
        }

        lines = [
            f"Graph generated at: {store.generated_at}",
            f"Total nodes: {store.G.number_of_nodes()}",
            f"Total edges: {store.G.number_of_edges()}",
            "",
            "Nodes by type:",
        ]
        for ntype, count in node_counts.items():
            lines.append(f"  {ntype}: {count}")
        lines.append("")
        lines.append("Edges by relation type:")
        for etype, count in edge_counts.items():
            lines.append(f"  {etype}: {count}")
        lines.append("")
        lines.append(f"Database tables: {len(store.tables_by_name)}")
        lines.append(f"Table names: {', '.join(sorted(store.tables_by_name.keys()))}")
        return "\n".join(lines)

    @mcp.tool()
    def search_nodes(
        node_type: str = "",
        name_pattern: str = "",
        urn_pattern: str = "",
        metadata_key: str = "",
        metadata_value: str = "",
        limit: int = 50,
        offset: int = 0,
    ) -> dict:
        """Search graph nodes by type, name, URN pattern, or metadata.

        Args:
            node_type: Filter by type (database, schema, table, column,
                       codebase, file, class, function).
            name_pattern: Substring match against common name fields
                          (table_name, class_name, function_name, etc.).
            urn_pattern: Substring match against the node URN.
            metadata_key: Filter to nodes that have this metadata key.
            metadata_value: If metadata_key is set, match this value substring.
            limit: Maximum results to return (default 50).
            offset: Number of matching results to skip for pagination (default 0).
        """
        if node_type:
            candidate_urns = store.nodes_by_type.get(node_type, [])
        else:
            candidate_urns = list(store.G.nodes)

        matched = []
        name_fields = [
            "table_name", "column_name", "schema_name", "class_name",
            "function_name", "file_path", "database_name", "repo_name",
        ]

        for urn in candidate_urns:
            attrs = store.G.nodes[urn]
            meta = attrs.get("metadata", {})

            if urn_pattern and urn_pattern.lower() not in urn.lower():
                continue
            if name_pattern:
                hit = any(
                    name_pattern.lower() in str(meta.get(f, "")).lower()
                    for f in name_fields
                )
                if not hit:
                    continue
            if metadata_key:
                val = meta.get(metadata_key)
                if val is None:
                    continue
                if metadata_value and metadata_value.lower() not in str(val).lower():
                    continue

            matched.append(urn)

        total = len(matched)
        page = matched[offset:offset + limit]
        remaining = max(0, total - offset - limit)

        return {
            "nodes": [store.node_dict(urn) for urn in page],
            "total": total,
            "remaining": remaining,
        }

    @mcp.tool()
    def get_node_details(urn: str) -> str:
        """Get full details for a specific node by URN, including all metadata
        and connected edges (both incoming and outgoing)."""
        node = store.node_dict(urn)
        if not node:
            return f"No node found with URN: {urn}"

        lines = [_format_node(node)]
        lines.append("")

        outgoing = list(store.G.out_edges(urn, data=True, keys=True))
        if outgoing:
            lines.append(f"Outgoing edges ({len(outgoing)}):")
            for _, to_urn, _key, data in outgoing:
                target = store.node_dict(to_urn)
                target_label = _node_label(target) if target else to_urn
                target_type = target["node_type"] if target else "?"
                lines.append(
                    f"  --[{data.get('relation_type', '?')}]--> [{target_type}] {target_label}"
                )
                for k, v in data.get("metadata", {}).items():
                    lines.append(f"      {k}: {v}")

        incoming = list(store.G.in_edges(urn, data=True, keys=True))
        if incoming:
            lines.append(f"Incoming edges ({len(incoming)}):")
            for from_urn, _, _key, data in incoming:
                source = store.node_dict(from_urn)
                source_label = _node_label(source) if source else from_urn
                source_type = source["node_type"] if source else "?"
                lines.append(
                    f"  <--[{data.get('relation_type', '?')}]-- [{source_type}] {source_label}"
                )
                for k, v in data.get("metadata", {}).items():
                    lines.append(f"      {k}: {v}")

        return "\n".join(lines)

    @mcp.tool()
    def get_neighbors(
        urn: str,
        direction: str = "both",
        edge_type: str = "",
    ) -> str:
        """Get all nodes connected to a given node.

        Args:
            urn: The URN of the center node.
            direction: 'outgoing', 'incoming', or 'both' (default).
            edge_type: Optional filter by relation type
                       (CONTAINS, DATA_TO_DATA, CODE_TO_DATA, etc.).
        """
        if urn not in store.G:
            return f"No node found with URN: {urn}"

        edge_list: list[tuple[str, str, dict]] = []  # (neighbor_urn, arrow, data)
        if direction in ("outgoing", "both"):
            for _, to_urn, data in store.G.out_edges(urn, data=True):
                if edge_type and data.get("relation_type") != edge_type:
                    continue
                arrow = f"--[{data.get('relation_type', '?')}]-->"
                edge_list.append((to_urn, arrow, data))
        if direction in ("incoming", "both"):
            for from_urn, _, data in store.G.in_edges(urn, data=True):
                if edge_type and data.get("relation_type") != edge_type:
                    continue
                arrow = f"<--[{data.get('relation_type', '?')}]--"
                edge_list.append((from_urn, arrow, data))

        if not edge_list:
            return f"No neighbors found for {urn} (direction={direction}, edge_type={edge_type or 'any'})."

        lines = [f"Neighbors of {urn} ({len(edge_list)} edges):"]
        for neighbor_urn, arrow, _data in edge_list:
            neighbor = store.node_dict(neighbor_urn)
            if neighbor:
                lines.append(f"  {arrow} [{neighbor['node_type']}] {_node_label(neighbor)}")
                lines.append(f"          URN: {neighbor_urn}")
            else:
                lines.append(f"  {arrow} {neighbor_urn}")

        return "\n".join(lines)
