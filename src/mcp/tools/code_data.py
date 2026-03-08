from __future__ import annotations

import networkx as nx

from mcp.server.fastmcp import FastMCP
from src.mcp._formatting import _node_label
from src.mcp.graph_store import GraphStore


def register(mcp: FastMCP, store: GraphStore) -> None:
    @mcp.tool()
    def find_code_for_table(table_name: str) -> str:
        """Given a database table name, find all code nodes (ORM classes and
        functions) that reference it via reads/writes/models edges. Shows detection
        method and confidence for each link."""
        table_urn = store.tables_by_name.get(table_name)
        if not table_urn:
            available = ", ".join(sorted(store.tables_by_name.keys())[:20])
            return f"No table found with name '{table_name}'. Available tables: {available}..."

        code_edges = [
            (from_urn, data)
            for from_urn, _, data in store.G.in_edges(table_urn, data=True)
            if data.get("edge_type") in {"reads", "writes", "models"}
        ]

        if not code_edges:
            return f"No code references found for table '{table_name}'."

        orm_edges = [(u, d) for u, d in code_edges if d.get("metadata", {}).get("detection_method") == "orm_tablename"]
        func_edges = [(u, d) for u, d in code_edges if d.get("metadata", {}).get("detection_method") == "orm_reference"]

        lines = [f"Code references for table '{table_name}' ({len(code_edges)} total):"]

        if orm_edges:
            lines.append(f"\nORM Models ({len(orm_edges)}):")
            for from_urn, data in orm_edges:
                source = store.node_dict(from_urn)
                if source:
                    lines.append(f"  {source['metadata'].get('class_name', '?')}")
                    lines.append(f"    File: {source['metadata'].get('file_path', '?')}")
                    lines.append(f"    URN: {from_urn}")
                    lines.append(f"    Confidence: {data.get('metadata', {}).get('confidence', '?')}")

        if func_edges:
            lines.append(f"\nFunctions ({len(func_edges)}):")
            for from_urn, data in func_edges:
                source = store.node_dict(from_urn)
                if source:
                    func_name = source["metadata"].get("function_name", "?")
                    file_path = source["metadata"].get("file_path", "?")
                    model = data.get("metadata", {}).get("referenced_model", "?")
                    lines.append(f"  {func_name}()")
                    lines.append(f"    File: {file_path}")
                    lines.append(f"    Via ORM model: {model}")
                    lines.append(f"    Confidence: {data.get('metadata', {}).get('confidence', '?')}")

        return "\n".join(lines)

    @mcp.tool()
    def find_tables_for_code(name: str) -> str:
        """Given a function or class name, find all database tables it
        references via reads/writes/models edges."""
        matching = []
        for ntype in ("function", "class"):
            key = "function_name" if ntype == "function" else "class_name"
            for urn in store.nodes_by_type.get(ntype, []):
                meta = store.G.nodes[urn].get("metadata", {})
                if meta.get(key) == name:
                    matching.append(store.node_dict(urn))

        if not matching:
            return f"No function or class found with name '{name}'."

        lines = []
        for node in matching:
            urn = node["urn"]
            code_edges = [
                (to_urn, data)
                for _, to_urn, data in store.G.out_edges(urn, data=True)
                if data.get("edge_type") in {"reads", "writes", "models"}
            ]

            label = _node_label(node)
            ntype = node["node_type"]
            lines.append(f"[{ntype}] {label}")
            lines.append(f"  URN: {urn}")
            lines.append(f"  File: {node['metadata'].get('file_path', '?')}")

            if code_edges:
                lines.append(f"  Tables referenced ({len(code_edges)}):")
                for to_urn, data in code_edges:
                    target = store.node_dict(to_urn)
                    t_name = target["metadata"].get("table_name", "?") if target else "?"
                    method = data.get("metadata", {}).get("detection_method", "?")
                    lines.append(f"    → {t_name} (method: {method})")
            else:
                lines.append("  No table references found.")
            lines.append("")

        return "\n".join(lines)

    @mcp.tool()
    def find_orphaned_tables() -> str:
        """Find all database tables that have no reads/writes/models edges pointing
        to them. These are potential orphaned resources with no known code
        references in the scanned codebase."""
        referenced_urns: set[str] = set()
        for edge_type in ("reads", "writes", "models"):
            for _from, to, _key in store.edges_by_type.get(edge_type, []):
                referenced_urns.add(to)

        orphaned = []
        for table_urn in store.nodes_by_type.get("table", []):
            if table_urn not in referenced_urns:
                orphaned.append(store.node_dict(table_urn))

        if not orphaned:
            return "No orphaned tables found — all tables have at least one code reference."

        lines = [f"Found {len(orphaned)} orphaned table(s) with no code references:"]
        for table in orphaned:
            name = table["metadata"].get("table_name", "?")
            schema = table["metadata"].get("schema_name", "?")
            lines.append(f"  {schema}.{name}")
            lines.append(f"    URN: {table['urn']}")

            data_edge_count = 0
            for _, _, d in store.G.out_edges(table["urn"], data=True):
                if d.get("edge_type") in {"references", "soft_reference"}:
                    data_edge_count += 1
            for _, _, d in store.G.in_edges(table["urn"], data=True):
                if d.get("edge_type") in {"references", "soft_reference"}:
                    data_edge_count += 1
            if data_edge_count:
                lines.append(f"    FK relationships: {data_edge_count}")

        return "\n".join(lines)

    @mcp.tool()
    def get_table_access_map(table_name: str = "") -> str:
        """Map tables to their code access patterns. For each table, shows
        ORM models and functions that access it.

        Args:
            table_name: Optional specific table name. If empty, shows map
                        for all tables that have code references.
        """
        table_map: dict[str, list[tuple[str, dict]]] = {}
        for edge_type in ("reads", "writes", "models"):
            for from_urn, to_urn, _key in store.edges_by_type.get(edge_type, []):
                target = store.node_dict(to_urn)
                if not target or target.get("node_type") != "table":
                    continue
                t_name = target["metadata"].get("table_name", "?")
                if table_name and t_name != table_name:
                    continue
                edge_data = store.G.edges[from_urn, to_urn, _key]
                table_map.setdefault(t_name, []).append((from_urn, edge_data))

        if not table_map:
            if table_name:
                return f"No code access patterns found for table '{table_name}'."
            return "No code access patterns found."

        lines = [f"Table access map ({len(table_map)} tables):"]
        for t_name in sorted(table_map.keys()):
            edges = table_map[t_name]
            orm_models = []
            functions = []
            for from_urn, data in edges:
                source = store.node_dict(from_urn)
                if not source:
                    continue
                method = data.get("metadata", {}).get("detection_method", "")
                if method == "orm_tablename":
                    orm_models.append(source)
                else:
                    functions.append((source, data))

            lines.append(f"\n  {t_name}:")
            if orm_models:
                model_names = [m["metadata"].get("class_name", "?") for m in orm_models]
                lines.append(f"    ORM models: {', '.join(model_names)}")
            if functions:
                lines.append(f"    Functions ({len(functions)}):")
                for func, data in functions:
                    func_name = func["metadata"].get("function_name", "?")
                    file_path = func["metadata"].get("file_path", "?")
                    via_model = data.get("metadata", {}).get("referenced_model", "?")
                    lines.append(f"      {func_name}() in {file_path} (via {via_model})")

        return "\n".join(lines)

    @mcp.tool()
    def find_data_hubs(min_tables: int = 3, limit: int = 10) -> str:
        """Find clusters of interconnected functions (via call edges)
        that collectively touch many database tables (via reads/writes/models).
        Useful for identifying the most complex and data-heavy parts of the
        codebase.

        Args:
            min_tables: Minimum number of tables a cluster must touch to be
                        included (default 3).
            limit: Maximum number of clusters to return (default 10).
        """
        c2c_triples = store.edges_by_type.get("calls", [])
        c2c_nodes: set[str] = set()
        c2c_graph = nx.DiGraph()
        for from_urn, to_urn, _key in c2c_triples:
            c2c_graph.add_edge(from_urn, to_urn)
            c2c_nodes.add(from_urn)
            c2c_nodes.add(to_urn)

        components: list[set[str]] = list(nx.weakly_connected_components(c2c_graph))

        c2d_triples = []
        for et in ("reads", "writes", "models"):
            c2d_triples.extend(store.edges_by_type.get(et, []))
        code_nodes_with_data = set()
        for from_urn, _to, _key in c2d_triples:
            code_nodes_with_data.add(from_urn)

        isolated_code = code_nodes_with_data - c2c_nodes
        for urn in isolated_code:
            components.append({urn})

        clusters = []
        for component in components:
            tables_by_func: dict[str, list[str]] = {}
            for func_urn in component:
                for _, to_urn, data in store.G.out_edges(func_urn, data=True):
                    if data.get("edge_type") not in {"reads", "writes", "models"}:
                        continue
                    target = store.node_dict(to_urn)
                    if target and target.get("node_type") == "table":
                        t_name = target["metadata"].get("table_name", to_urn)
                        tables_by_func.setdefault(t_name, []).append(func_urn)

            if len(tables_by_func) >= min_tables:
                clusters.append((component, tables_by_func))

        if not clusters:
            return f"No data hub clusters found with >= {min_tables} tables."

        clusters.sort(key=lambda x: len(x[1]), reverse=True)
        clusters = clusters[:limit]

        lines = [f"Data Hub Clusters ({len(clusters)} found):"]

        for idx, (component, tables_by_func) in enumerate(clusters, 1):
            lines.append(f"\nCluster {idx}: {len(component)} functions → {len(tables_by_func)} tables")

            lines.append("  Functions:")
            for func_urn in sorted(component):
                node = store.node_dict(func_urn)
                if node:
                    fname = node["metadata"].get("function_name", "?")
                    fpath = node["metadata"].get("file_path", "?")
                    lines.append(f"    {fname}() in {fpath}")

            internal_edges = []
            for func_urn in component:
                if func_urn not in c2c_graph:
                    continue
                for _, to_urn in c2c_graph.out_edges(func_urn):
                    if to_urn in component:
                        from_node = store.node_dict(func_urn)
                        to_node = store.node_dict(to_urn)
                        from_name = from_node["metadata"].get("function_name", "?") if from_node else "?"
                        to_name = to_node["metadata"].get("function_name", "?") if to_node else "?"
                        internal_edges.append(f"    {from_name} --[calls]--> {to_name}")

            if internal_edges:
                lines.append("  Call chain:")
                lines.extend(internal_edges)

            lines.append("  Tables accessed:")
            for t_name in sorted(tables_by_func.keys()):
                func_urns = tables_by_func[t_name]
                func_names = []
                for fu in func_urns:
                    n = store.node_dict(fu)
                    func_names.append(n["metadata"].get("function_name", "?") if n else "?")
                lines.append(f"    {t_name} (via: {', '.join(func_names)})")

        return "\n".join(lines)
