from __future__ import annotations

import uuid
from datetime import UTC, datetime

from mcp.server.fastmcp import FastMCP
from src.graph.graph_models import EdgeType
from src.mcp._formatting import _node_label
from src.mcp.graph_store import EDGE_NAMESPACE, GraphStore

# Valid edge types that soft links can create.
SOFT_LINK_EDGE_TYPES = frozenset({
    EdgeType.BUILDS,
    EdgeType.READS,
    EdgeType.WRITES,
    EdgeType.REFERENCES,
    EdgeType.MODELS,
    EdgeType.CALLS,
    EdgeType.DEPENDS_ON,
    EdgeType.CONTAINS,
    EdgeType.SOFT_REFERENCE,
})

# Confidence levels mapped to numeric scores for storage/comparison.
CONFIDENCE_LEVELS = {
    "VERY_HIGH": 0.95,
    "HIGH": 0.8,
    "MEDIUM": 0.6,
    "LOW": 0.4,
}


def register(mcp: FastMCP, store: GraphStore) -> None:
    @mcp.tool()
    def add_soft_link(
        from_urn: str,
        to_urn: str,
        edge_type: str = "soft_reference",
        confidence: str = "MEDIUM",
        note: str = "",
    ) -> str:
        """Create an edge between two nodes. The link is persisted to
        soft_links.json and immediately added to the in-memory graph.

        Use this to manually establish relationships that automated
        scanning could not detect (e.g. Dockerfile → ECR repository,
        code → S3 bucket).

        Args:
            from_urn: Source node URN (must exist in graph).
            to_urn: Target node URN (must exist in graph).
            edge_type: Relationship type. One of: builds, reads, writes,
                       references, models, calls, depends_on, contains,
                       soft_reference (default).
            confidence: Confidence level: VERY_HIGH, HIGH, MEDIUM (default),
                        or LOW.
            note: Human-readable reason for the link — include your
                  evidence so future reviewers understand the rationale.
        """
        if from_urn not in store.G:
            return f"Error: from_urn not found in graph: {from_urn}"
        if to_urn not in store.G:
            return f"Error: to_urn not found in graph: {to_urn}"
        if edge_type not in SOFT_LINK_EDGE_TYPES:
            return (
                f"Error: invalid edge_type '{edge_type}'. "
                f"Valid types: {', '.join(sorted(SOFT_LINK_EDGE_TYPES))}"
            )
        confidence_upper = confidence.upper()
        if confidence_upper not in CONFIDENCE_LEVELS:
            return (
                f"Error: invalid confidence '{confidence}'. "
                f"Valid levels: {', '.join(CONFIDENCE_LEVELS)}"
            )
        confidence_score = CONFIDENCE_LEVELS[confidence_upper]

        for existing in store.soft_links:
            if (existing["from_urn"] == from_urn
                    and existing["to_urn"] == to_urn
                    and existing.get("edge_type") == edge_type):
                return f"Error: soft link already exists (id={existing['id']})"

        link_id = str(uuid.uuid4())
        edge_key = str(uuid.uuid5(
            EDGE_NAMESPACE, f"{from_urn}:{to_urn}:{edge_type}"
        ))
        org_id = store.G.nodes[from_urn].get("organization_id")

        link = {
            "id": link_id,
            "from_urn": from_urn,
            "to_urn": to_urn,
            "edge_type": edge_type,
            "detection_method": "soft_link",
            "confidence": confidence_score,
            "confidence_level": confidence_upper,
            "note": note,
            "created_at": datetime.now(UTC).isoformat(),
        }

        store.soft_links.append(link)
        store.G.add_edge(
            from_urn, to_urn, key=edge_key,
            edge_type=edge_type,
            metadata={
                "detection_method": "soft_link",
                "confidence": confidence_score,
                "confidence_level": confidence_upper,
                "note": note,
            },
            organization_id=org_id,
        )
        store.edges_by_type.setdefault(edge_type, []).append(
            (from_urn, to_urn, edge_key)
        )
        store._save_soft_links()

        from_label = _node_label(store.node_dict(from_urn))
        to_label = _node_label(store.node_dict(to_urn))
        return (
            f"Soft link created (id={link_id}):\n"
            f"  {from_label} --[{edge_type}]--> {to_label}\n"
            f"  confidence={confidence_upper}, note={note!r}"
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
            level = link.get("confidence_level", "")
            score = link.get("confidence", "?")
            lines.append(f"  confidence: {level} ({score})" if level else f"  confidence: {score}")
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
        edge_type = target_link.get("edge_type", EdgeType.SOFT_REFERENCE)
        edge_key = str(uuid.uuid5(
            EDGE_NAMESPACE, f"{from_urn}:{to_urn}:{edge_type}"
        ))

        if store.G.has_edge(from_urn, to_urn, key=edge_key):
            store.G.remove_edge(from_urn, to_urn, key=edge_key)

        edge_tuple = (from_urn, to_urn, edge_key)
        if edge_type in store.edges_by_type:
            try:
                store.edges_by_type[edge_type].remove(edge_tuple)
            except ValueError:
                pass

        store.soft_links.remove(target_link)
        store._save_soft_links()

        from_node = store.node_dict(from_urn)
        to_node = store.node_dict(to_urn)
        from_label = _node_label(from_node) if from_node else from_urn
        to_label = _node_label(to_node) if to_node else to_urn
        return f"Soft link removed (id={soft_link_id}): {from_label} → {to_label}"
