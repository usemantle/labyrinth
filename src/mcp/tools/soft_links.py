from __future__ import annotations

import uuid
from datetime import datetime, timezone

from mcp.server.fastmcp import FastMCP

from src.mcp._formatting import _node_label
from src.mcp.graph_store import EDGE_NAMESPACE, GraphStore


def register(mcp: FastMCP, store: GraphStore) -> None:
    @mcp.tool()
    def add_soft_link(
        from_urn: str,
        to_urn: str,
        confidence: float = 0.7,
        note: str = "",
    ) -> str:
        """Manually create a CODE_TO_DATA edge between two URNs. The link is
        persisted to soft_links.json and immediately added to the in-memory
        graph.

        Args:
            from_urn: Source node URN (must exist in graph).
            to_urn: Target node URN (must exist in graph).
            confidence: Confidence score between 0.0 and 1.0 (default 0.7).
            note: Human-readable reason for the link.
        """
        if from_urn not in store.G:
            return f"Error: from_urn not found in graph: {from_urn}"
        if to_urn not in store.G:
            return f"Error: to_urn not found in graph: {to_urn}"

        for existing in store.soft_links:
            if existing["from_urn"] == from_urn and existing["to_urn"] == to_urn:
                return f"Error: soft link already exists (id={existing['id']})"

        relation_type = "CODE_TO_DATA"
        link_id = str(uuid.uuid4())
        edge_key = str(uuid.uuid5(
            EDGE_NAMESPACE, f"{from_urn}:{to_urn}:{relation_type}"
        ))
        org_id = store.G.nodes[from_urn].get("organization_id")

        link = {
            "id": link_id,
            "from_urn": from_urn,
            "to_urn": to_urn,
            "relation_type": relation_type,
            "detection_method": "soft_link",
            "confidence": confidence,
            "note": note,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        store.soft_links.append(link)
        store.G.add_edge(
            from_urn, to_urn, key=edge_key,
            relation_type=relation_type,
            metadata={
                "detection_method": "soft_link",
                "confidence": confidence,
                "note": note,
            },
            organization_id=org_id,
        )
        store.edges_by_type.setdefault(relation_type, []).append(
            (from_urn, to_urn, edge_key)
        )
        store._save_soft_links()

        from_label = _node_label(store.node_dict(from_urn))
        to_label = _node_label(store.node_dict(to_urn))
        return (
            f"Soft link created (id={link_id}):\n"
            f"  {from_label} --[CODE_TO_DATA]--> {to_label}\n"
            f"  confidence={confidence}, note={note!r}"
        )

    @mcp.tool()
    def list_soft_links() -> str:
        """List all manually-created soft links with their IDs, node labels,
        confidence scores, and notes."""
        if not store.soft_links:
            return "No soft links defined."

        lines = [f"Soft links ({len(store.soft_links)}):"]
        for link in store.soft_links:
            from_node = store.node_dict(link["from_urn"])
            to_node = store.node_dict(link["to_urn"])
            from_label = _node_label(from_node) if from_node else link["from_urn"]
            to_label = _node_label(to_node) if to_node else link["to_urn"]
            lines.append(f"\n  id: {link['id']}")
            lines.append(f"  from: [{from_node['node_type'] if from_node else '?'}] {from_label}")
            lines.append(f"        {link['from_urn']}")
            lines.append(f"  to:   [{to_node['node_type'] if to_node else '?'}] {to_label}")
            lines.append(f"        {link['to_urn']}")
            lines.append(f"  confidence: {link.get('confidence', '?')}")
            lines.append(f"  note: {link.get('note', '')}")
            lines.append(f"  created_at: {link.get('created_at', '?')}")

        return "\n".join(lines)

    @mcp.tool()
    def remove_soft_link(soft_link_id: str) -> str:
        """Remove a soft link by its UUID. The edge is removed from the
        in-memory graph and the change is persisted to soft_links.json.

        Args:
            soft_link_id: UUID of the soft link to remove.
        """
        target_link = None
        for link in store.soft_links:
            if link["id"] == soft_link_id:
                target_link = link
                break

        if target_link is None:
            return f"Error: no soft link found with id={soft_link_id}"

        from_urn = target_link["from_urn"]
        to_urn = target_link["to_urn"]
        relation_type = target_link.get("relation_type", "CODE_TO_DATA")
        edge_key = str(uuid.uuid5(
            EDGE_NAMESPACE, f"{from_urn}:{to_urn}:{relation_type}"
        ))

        if store.G.has_edge(from_urn, to_urn, key=edge_key):
            store.G.remove_edge(from_urn, to_urn, key=edge_key)

        edge_tuple = (from_urn, to_urn, edge_key)
        if relation_type in store.edges_by_type:
            try:
                store.edges_by_type[relation_type].remove(edge_tuple)
            except ValueError:
                pass

        store.soft_links.remove(target_link)
        store._save_soft_links()

        from_node = store.node_dict(from_urn)
        to_node = store.node_dict(to_urn)
        from_label = _node_label(from_node) if from_node else from_urn
        to_label = _node_label(to_node) if to_node else to_urn
        return f"Soft link removed (id={soft_link_id}): {from_label} → {to_label}"
