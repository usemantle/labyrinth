"""Security-focused MCP tools for the Labyrinth knowledge graph.

Provides tools for:
- Sensitive data discovery (Feature 1)
- Dependency-to-code linking (Feature 2)
- Database permission analysis (Feature 4)
- Blast radius analysis (Feature 5)
"""

from __future__ import annotations

import collections
from typing import TYPE_CHECKING

from mcp.server.fastmcp import FastMCP

if TYPE_CHECKING:
    from src.mcp.graph_store import GraphStore


def _bfs(
    store: GraphStore,
    start: str,
    max_depth: int,
    follow_types: set[str],
    *,
    reverse: bool = False,
) -> dict[str, int]:
    """BFS traversal returning {urn: depth} for all reachable nodes.

    When reverse=True, follows incoming edges instead of outgoing.
    """
    visited: dict[str, int] = {}
    queue = collections.deque([(start, 0)])

    while queue:
        current, depth = queue.popleft()
        if current in visited or depth > max_depth:
            continue
        visited[current] = depth

        edges = store.G.in_edges(current, data=True) if reverse else store.G.out_edges(current, data=True)
        for edge in edges:
            neighbor = edge[0] if reverse else edge[1]
            if edge[2].get("relation_type") in follow_types and neighbor not in visited:
                queue.append((neighbor, depth + 1))

    return visited


def _node_label(node: dict) -> str:
    """Extract a human-readable label from a node dict."""
    meta = node["metadata"]
    return (
        meta.get("function_name")
        or meta.get("class_name")
        or meta.get("table_name")
        or meta.get("column_name")
        or meta.get("path_pattern")
        or meta.get("role_name")
        or node["urn"]
    )


def _code_label(node: dict) -> str:
    """Format a code node as 'name in file_path'."""
    meta = node["metadata"]
    name = meta.get("function_name") or meta.get("class_name", "?")
    fp = meta.get("file_path", "?")
    return f"{name} in {fp}"


def _find_dep_node(store: GraphStore, package_name: str) -> str | None:
    """Find a dependency node URN by package name (case-insensitive)."""
    for urn in store.nodes_by_type.get("dependency", []):
        meta = store.G.nodes[urn].get("metadata", {})
        if meta.get("package_name", "").lower() == package_name.lower():
            return urn
    return None


def _incoming_depends_on(store: GraphStore, dep_urn: str) -> list[tuple[str, dict]]:
    """Find all incoming DEPENDS_ON edges to a dependency node."""
    return [
        (from_urn, data)
        for from_urn, _, data in store.G.in_edges(dep_urn, data=True)
        if data.get("relation_type") == "DEPENDS_ON"
    ]


def register(mcp: FastMCP, store: GraphStore) -> None:

    # ── Feature 1: Sensitive Data ─────────────────────────────────────

    @mcp.tool()
    def find_sensitive_data(category: str = "") -> str:
        """Find all nodes tagged with DATA_SENSITIVITY metadata.

        Args:
            category: Optional prefix filter (e.g. 'pii', 'secret', 'financial').
                      If empty, returns all sensitive nodes.
        """
        results = []
        for urn in store.G.nodes():
            meta = store.G.nodes[urn].get("metadata", {})
            sensitivity = meta.get("data_sensitivity")
            if not sensitivity:
                continue
            tags = sensitivity.split(",")
            if category and not any(t.startswith(category) for t in tags):
                continue
            node = store.node_dict(urn)
            if node:
                results.append(node)

        if not results:
            return f"No sensitive data found{' for category: ' + category if category else ''}."

        lines = [f"Sensitive data nodes ({len(results)} found):"]
        for node in results:
            meta = node["metadata"]
            label = _node_label(node)
            parent_info = ""
            if meta.get("column_name") and node.get("parent_urn"):
                parent = store.node_dict(node["parent_urn"])
                if parent:
                    parent_info = f" in {parent['metadata'].get('table_name', '?')}"
            lines.append(f"\n  [{node['node_type']}] {label}{parent_info}")
            lines.append(f"    Sensitivity: {meta.get('data_sensitivity', '?')}")
            lines.append(f"    URN: {node['urn']}")
        return "\n".join(lines)

    @mcp.tool()
    def trace_sensitive_data_access(table_name: str) -> str:
        """For a table with sensitive columns, show all code that accesses it.

        Args:
            table_name: Name of the database table to trace.
        """
        table_urn = store.tables_by_name.get(table_name)
        if not table_urn:
            return f"No table found with name '{table_name}'."

        table_node = store.node_dict(table_urn)
        sensitivity = (table_node or {}).get("metadata", {}).get("data_sensitivity")

        # Find sensitive columns
        sensitive_cols = []
        for _, to_urn, data in store.G.out_edges(table_urn, data=True):
            if data.get("relation_type") != "CONTAINS":
                continue
            col_node = store.node_dict(to_urn)
            if col_node and col_node["metadata"].get("data_sensitivity"):
                sensitive_cols.append(col_node)

        # Find code accessing this table
        code_urns = [
            from_urn
            for from_urn, _, data in store.G.in_edges(table_urn, data=True)
            if data.get("relation_type") == "CODE_TO_DATA"
        ]

        lines = [f"Sensitive data access for table '{table_name}':"]
        if sensitivity:
            lines.append(f"  Table sensitivity: {sensitivity}")
        if sensitive_cols:
            lines.append(f"  Sensitive columns ({len(sensitive_cols)}):")
            for col in sensitive_cols:
                lines.append(f"    {col['metadata'].get('column_name', '?')}: {col['metadata'].get('data_sensitivity', '?')}")
        else:
            lines.append("  No sensitive columns found.")

        if code_urns:
            lines.append(f"\n  Code accessing this table ({len(code_urns)}):")
            for from_urn in code_urns:
                source = store.node_dict(from_urn)
                if source:
                    lines.append(f"    {_code_label(source)}")
        else:
            lines.append("\n  No code references found.")

        return "\n".join(lines)

    # ── Feature 2: Dependency-to-Code Linking ─────────────────────────

    @mcp.tool()
    def find_code_using_dependency(package_name: str) -> str:
        """Find all code files that import a given dependency package.

        Args:
            package_name: The package name (e.g. 'requests', 'flask').
        """
        dep_urn = _find_dep_node(store, package_name)
        if not dep_urn:
            return f"No dependency found with name '{package_name}'."

        dep_meta = store.G.nodes[dep_urn].get("metadata", {})
        lines = [f"Code using '{package_name}' (v{dep_meta.get('package_version', '?')}):"]

        cve_ids = dep_meta.get("cve_ids")
        if cve_ids:
            lines.append(f"  WARNING - Known CVEs: {cve_ids}")

        dep_edges = _incoming_depends_on(store, dep_urn)
        if dep_edges:
            lines.append(f"\n  Files importing this package ({len(dep_edges)}):")
            for from_urn, data in dep_edges:
                source = store.node_dict(from_urn)
                if source:
                    fp = source["metadata"].get("file_path", source["urn"])
                    import_name = data.get("metadata", {}).get("import_name", "?")
                    lines.append(f"    {fp} (import: {import_name})")
        else:
            lines.append("\n  No code files found importing this package.")

        return "\n".join(lines)

    @mcp.tool()
    def find_vulnerable_code() -> str:
        """Find all dependencies with known CVEs and the code files that use them."""
        vuln_deps = []
        for urn in store.nodes_by_type.get("dependency", []):
            meta = store.G.nodes[urn].get("metadata", {})
            if meta.get("cve_ids"):
                vuln_deps.append((urn, meta))

        if not vuln_deps:
            return "No vulnerable dependencies found."

        lines = [f"Vulnerable dependencies ({len(vuln_deps)} found):"]
        for dep_urn, meta in vuln_deps:
            pkg = meta.get("package_name", "?")
            ver = meta.get("package_version", "?")
            cves = meta.get("cve_ids", "?")
            lines.append(f"\n  {pkg}=={ver}")
            lines.append(f"    CVEs: {cves}")

            dep_edges = _incoming_depends_on(store, dep_urn)
            if dep_edges:
                lines.append(f"    Affected files ({len(dep_edges)}):")
                for from_urn, _data in dep_edges:
                    source = store.node_dict(from_urn)
                    if source:
                        fp = source["metadata"].get("file_path", source["urn"])
                        lines.append(f"      {fp}")
            else:
                lines.append("    No code files found importing this package.")

        return "\n".join(lines)

    # ── Feature 4: Database Permissions ───────────────────────────────

    @mcp.tool()
    def find_database_permissions(table_name: str = "", role_name: str = "") -> str:
        """Show database role permissions (PRINCIPAL_TO_DATA edges).

        Args:
            table_name: Optional filter by table name.
            role_name: Optional filter by role name.
        """
        p2d_edges = store.edges_by_type.get("PRINCIPAL_TO_DATA", [])
        if not p2d_edges:
            return "No database permission data found. Ensure PostgreSQL role/grant discovery is enabled."

        results: list[tuple[dict, dict, dict]] = []
        for from_urn, to_urn, key in p2d_edges:
            role_node = store.node_dict(from_urn)
            data_node = store.node_dict(to_urn)
            edge_data = store.G.edges[from_urn, to_urn, key]

            if not role_node or not data_node:
                continue

            if role_name and role_node["metadata"].get("role_name") != role_name:
                continue
            if table_name and data_node["metadata"].get("table_name") != table_name:
                continue

            results.append((role_node, data_node, edge_data))

        if not results:
            filters = []
            if table_name:
                filters.append(f"table='{table_name}'")
            if role_name:
                filters.append(f"role='{role_name}'")
            return f"No permissions found{' for ' + ', '.join(filters) if filters else ''}."

        lines = [f"Database permissions ({len(results)} grants):"]
        for role_node, data_node, edge_data in results:
            r_name = role_node["metadata"].get("role_name", "?")
            t_name = data_node["metadata"].get("table_name", "?")
            privilege = edge_data.get("metadata", {}).get("privilege", "?")
            is_super = role_node["metadata"].get("role_superuser", False)
            is_grantable = edge_data.get("metadata", {}).get("is_grantable", False)

            flags = []
            if is_super:
                flags.append("SUPERUSER")
            if is_grantable:
                flags.append("WITH GRANT OPTION")
            flag_str = f" [{', '.join(flags)}]" if flags else ""

            lines.append(f"  {r_name} -> {privilege} on {t_name}{flag_str}")

        return "\n".join(lines)

    # ── Feature 5: Blast Radius Analysis ──────────────────────────────

    @mcp.tool()
    def blast_radius(urn: str, max_depth: int = 5) -> str:
        """Analyze the blast radius from a compromised node.
        Starting from any node, follows outgoing edges to find all
        reachable code, data, and dependencies.

        Args:
            urn: URN of the starting node.
            max_depth: Maximum traversal depth (default 5).
        """
        if urn not in store.G:
            return f"No node found with URN: {urn}"

        follow_types = {"CODE_TO_CODE", "CODE_TO_DATA", "DATA_TO_DATA", "DEPENDS_ON", "CONTAINS"}
        visited = _bfs(store, urn, max_depth, follow_types)

        code_nodes = []
        data_nodes = []
        dep_nodes = []
        sensitive_count = 0
        cve_count = 0

        for node_urn, depth in visited.items():
            if node_urn == urn:
                continue
            node = store.node_dict(node_urn)
            if not node:
                continue

            ntype = node["node_type"]
            meta = node["metadata"]

            if ntype in ("function", "class"):
                code_nodes.append((node, depth))
            elif ntype in ("table", "column", "s3_bucket", "s3_prefix", "s3_object"):
                data_nodes.append((node, depth))
                if meta.get("data_sensitivity"):
                    sensitive_count += 1
            elif ntype == "dependency":
                dep_nodes.append((node, depth))
                if meta.get("cve_ids"):
                    cve_count += 1

        start_node = store.node_dict(urn)
        start_label = _node_label(start_node) if start_node else urn

        lines = [f"Blast radius from '{start_label}':"]
        lines.append(f"  Total reachable: {len(visited) - 1} nodes")
        lines.append(f"  Sensitive data nodes: {sensitive_count}")
        lines.append(f"  CVE-affected dependencies: {cve_count}")

        if code_nodes:
            lines.append(f"\n  Directly affected code ({len(code_nodes)}):")
            for node, depth in sorted(code_nodes, key=lambda x: x[1]):
                lines.append(f"    [depth={depth}] {_code_label(node)}")

        if data_nodes:
            lines.append(f"\n  Data at risk ({len(data_nodes)}):")
            for node, depth in sorted(data_nodes, key=lambda x: x[1]):
                meta = node["metadata"]
                name = meta.get("table_name") or meta.get("column_name") or meta.get("path_pattern", "?")
                sens = meta.get("data_sensitivity")
                sens_str = f" [SENSITIVE: {sens}]" if sens else ""
                lines.append(f"    [depth={depth}] {name}{sens_str}")

        if dep_nodes:
            lines.append(f"\n  Dependencies ({len(dep_nodes)}):")
            for node, depth in sorted(dep_nodes, key=lambda x: x[1]):
                meta = node["metadata"]
                pkg = meta.get("package_name", "?")
                ver = meta.get("package_version", "?")
                cves = meta.get("cve_ids")
                cve_str = f" [CVEs: {cves}]" if cves else ""
                lines.append(f"    [depth={depth}] {pkg}=={ver}{cve_str}")

        return "\n".join(lines)

    @mcp.tool()
    def reverse_blast_radius(urn: str, max_depth: int = 5) -> str:
        """Trace backwards from a data node to find all code, principals,
        and endpoints that can access it.

        Args:
            urn: URN of the data node to trace from.
            max_depth: Maximum traversal depth (default 5).
        """
        if urn not in store.G:
            return f"No node found with URN: {urn}"

        follow_types = {"CODE_TO_DATA", "CODE_TO_CODE", "PRINCIPAL_TO_DATA", "CONTAINS"}
        visited = _bfs(store, urn, max_depth, follow_types, reverse=True)

        code_paths = []
        principals = []
        endpoints = []

        for node_urn, depth in visited.items():
            if node_urn == urn:
                continue
            node = store.node_dict(node_urn)
            if not node:
                continue

            ntype = node["node_type"]
            meta = node["metadata"]

            if ntype == "db_role":
                principals.append((node, depth))
            elif ntype in ("function", "class"):
                code_paths.append((node, depth))
                if meta.get("route_path"):
                    endpoints.append((node, depth))

        start_node = store.node_dict(urn)
        start_label = _node_label(start_node) if start_node else urn

        lines = [f"Reverse blast radius for '{start_label}':"]
        lines.append(f"  Total upstream nodes: {len(visited) - 1}")

        if code_paths:
            lines.append(f"\n  Code paths to this data ({len(code_paths)}):")
            for node, depth in sorted(code_paths, key=lambda x: x[1]):
                lines.append(f"    [depth={depth}] {_code_label(node)}")

        if principals:
            lines.append(f"\n  Principals with access ({len(principals)}):")
            for node, depth in sorted(principals, key=lambda x: x[1]):
                meta = node["metadata"]
                r_name = meta.get("role_name", "?")
                is_super = meta.get("role_superuser", False)
                flag = " [SUPERUSER]" if is_super else ""
                lines.append(f"    [depth={depth}] {r_name}{flag}")

        if endpoints:
            lines.append(f"\n  Endpoints exposing this data ({len(endpoints)}):")
            for node, depth in sorted(endpoints, key=lambda x: x[1]):
                meta = node["metadata"]
                method = meta.get("http_method", "?")
                path = meta.get("full_route_path") or meta.get("route_path", "?")
                auth_scheme = meta.get("auth_scheme")
                auth_str = f"auth: {auth_scheme}" if auth_scheme else "no auth detected"
                lines.append(f"    [depth={depth}] {method} {path} ({auth_str})")

        return "\n".join(lines)
