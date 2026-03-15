"""Candidate data model and check-stage filter."""

from __future__ import annotations

from dataclasses import dataclass

from src.mcp.graph_store import GraphStore


@dataclass
class Candidate:
    """A node suspected of missing a soft link."""

    source_urn: str
    source_node_type: str
    source_metadata: dict
    heuristic_name: str
    target_edge_type: str
    target_node_type: str
    skill_file: str
    edge_direction: str = "outgoing"  # "outgoing" or "incoming"


@dataclass
class CandidateResult:
    """Outcome of investigating a single candidate."""

    candidate: Candidate
    outcome: str  # "linked", "rejected", "error"
    soft_link_id: str | None
    note: str


def filter_already_linked(candidates: list[Candidate], store: GraphStore) -> list[Candidate]:
    """Remove candidates whose source node already has an outgoing edge of the target type.

    NOTE: This is a coarse filter. A node can legitimately have multiple soft
    links of the same type (e.g. a function that writes to two S3 buckets).
    A future improvement should check specific target URNs, not just edge-type
    presence.
    """
    unlinked: list[Candidate] = []
    with store.lock:
        for c in candidates:
            has_edge = False
            if c.edge_direction == "incoming":
                edges = store.G.in_edges(c.source_urn, data=True)
            else:
                edges = store.G.out_edges(c.source_urn, data=True)
            for _, _, data in edges:
                if data.get("edge_type") == c.target_edge_type:
                    has_edge = True
                    break
            if not has_edge:
                unlinked.append(c)
    return unlinked
