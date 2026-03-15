from __future__ import annotations

import collections

import networkx as nx

from mcp.server.fastmcp import FastMCP
from src.graph.graph_models import EdgeType
from src.mcp._formatting import _lookup_edge_label, _node_label
from src.mcp.graph_store import GraphStore


def register(mcp: FastMCP, store: GraphStore) -> None:
    @mcp.tool()
    def trace_data_path(
        start_urn: str,
        edge_types: str = "references,soft_reference,reads,writes,models",
        max_depth: int = 10,
        direction: str = "outgoing",
    ) -> str:
        """BFS traversal from a starting node following specified edge types.
        Returns all reachable nodes with their distance from the start.
        Useful for data flow analysis and impact assessment.

        Args:
            start_urn: URN of the starting node.
            edge_types: Comma-separated edge types to follow
                        (e.g., 'references,reads,writes,models').
            max_depth: Maximum traversal depth (default 10).
            direction: 'outgoing' (default), 'incoming', or 'both'.
        """
        if start_urn not in store.G:
            return f"No node found with URN: {start_urn}"

        allowed = set(edge_types.split(","))
        visited: dict[str, int] = {}
        queue = collections.deque([(start_urn, 0)])

        while queue:
            current, depth = queue.popleft()
            if current in visited or depth > max_depth:
                continue
            visited[current] = depth

            if direction in ("outgoing", "both"):
                for _, to_urn, data in store.G.out_edges(current, data=True):
                    if data.get("edge_type") in allowed and to_urn not in visited:
                        queue.append((to_urn, depth + 1))
            if direction in ("incoming", "both"):
                for from_urn, _, data in store.G.in_edges(current, data=True):
                    if data.get("edge_type") in allowed and from_urn not in visited:
                        queue.append((from_urn, depth + 1))

        if len(visited) <= 1:
            return f"No reachable nodes from {start_urn} via edge types: {edge_types}"

        lines = [f"Reachable nodes from {start_urn} ({len(visited) - 1} found):"]
        for urn, depth in sorted(visited.items(), key=lambda x: x[1]):
            if urn == start_urn:
                continue
            node = store.node_dict(urn)
            if node:
                lines.append(f"  depth={depth}  [{node['node_type']}] {_node_label(node)}")
                lines.append(f"           URN: {urn}")
            else:
                lines.append(f"  depth={depth}  {urn}")

        return "\n".join(lines)

    @mcp.tool()
    def get_subgraph(center_urn: str, hops: int = 2) -> str:
        """Extract a subgraph around a center node within N hops in all
        directions. Returns both nodes and edges. Useful for understanding
        the local neighborhood of any resource.

        Note: 'contains' edges are excluded from traversal because they
        represent organizational hierarchy (e.g. aws_account contains all
        its resources). Following them would pull in entire subtrees of
        unrelated siblings from root container nodes.

        Args:
            center_urn: URN of the center node.
            hops: Number of hops to expand (default 2).
        """
        if center_urn not in store.G:
            return f"No node found with URN: {center_urn}"

        # Exclude 'contains' edges — they are organizational, not semantic.
        semantic_view = nx.subgraph_view(
            store.G,
            filter_edge=lambda u, v, k: store.G.edges[u, v, k].get("edge_type") != EdgeType.CONTAINS,
        )
        undirected = semantic_view.to_undirected(as_view=True)
        ego_nodes = set(nx.ego_graph(undirected, center_urn, radius=hops).nodes())

        sub = store.G.subgraph(ego_nodes)

        type_counts: dict[str, int] = {}
        for urn in sub.nodes():
            t = store.G.nodes[urn].get("node_type", "unknown")
            type_counts[t] = type_counts.get(t, 0) + 1

        lines = [
            f"Subgraph around {center_urn} ({hops} hops):",
            f"  Nodes: {sub.number_of_nodes()}",
            f"  Edges: {sub.number_of_edges()}",
            f"  Node types: {', '.join(f'{k}={v}' for k, v in sorted(type_counts.items()))}",
            "",
            "Nodes:",
        ]
        for urn in sub.nodes():
            node = store.node_dict(urn)
            if node:
                lines.append(f"  [{node['node_type']}] {_node_label(node)}")
                lines.append(f"    URN: {urn}")

        lines.append("")
        lines.append("Edges:")
        for from_urn, to_urn, data in sub.edges(data=True):
            from_node = store.node_dict(from_urn)
            to_node = store.node_dict(to_urn)
            from_label = _node_label(from_node) if from_node else from_urn
            to_label = _node_label(to_node) if to_node else to_urn
            lines.append(f"  {from_label} --[{data.get('edge_type', '?')}]--> {to_label}")

        return "\n".join(lines)

    @mcp.tool()
    def find_shortest_path(
        from_urn: str,
        to_urn: str,
        edge_types: str = "",
    ) -> str:
        """Find the shortest path between two nodes in the graph, ignoring
        edge direction. Useful for understanding how two resources are
        related even if separated by many hops.

        Args:
            from_urn: URN of the starting node.
            to_urn: URN of the target node.
            edge_types: Optional comma-separated edge types to restrict
                        traversal (e.g., 'reads,writes,calls').
                        If empty, all edge types are followed.
        """
        if from_urn not in store.G:
            return f"No node found with URN: {from_urn}"
        if to_urn not in store.G:
            return f"No node found with URN: {to_urn}"

        if edge_types:
            allowed = set(edge_types.split(","))
            view = nx.subgraph_view(
                store.G,
                filter_edge=lambda u, v, k: store.G.edges[u, v, k].get("edge_type") in allowed,
            )
        else:
            view = store.G

        undirected = view.to_undirected()

        try:
            path = nx.shortest_path(undirected, from_urn, to_urn)
        except nx.NetworkXNoPath:
            return f"No path found between {from_urn} and {to_urn}."
        except nx.NodeNotFound as e:
            return str(e)

        hop_count = len(path) - 1
        lines = [f"Shortest path ({hop_count} hop{'s' if hop_count != 1 else ''}):"]

        for i, urn in enumerate(path):
            node = store.node_dict(urn)
            if node:
                lines.append(f"  [{node['node_type']}] {_node_label(node)}  ({urn})")
            else:
                lines.append(f"  {urn}")

            if i < len(path) - 1:
                next_urn = path[i + 1]
                edge_label = _lookup_edge_label(store.G, urn, next_urn)
                lines.append(f"  {edge_label}")

        return "\n".join(lines)
