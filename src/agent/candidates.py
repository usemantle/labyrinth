"""Candidate data model and check-stage filter."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from src.mcp.graph_store import GraphStore

CANDIDATE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "labyrinth:candidate")


def candidate_id(
    source_urn: str,
    heuristic_name: str,
    dest_urn: str | None = None,
) -> str:
    """Return a deterministic UUID5 for a candidate.

    Source-only candidates are keyed by ``(source_urn, heuristic_name)``.
    Path-linked candidates additionally include ``dest_urn`` so a single
    source linked to multiple dests yields distinct IDs.
    """
    if dest_urn is None:
        seed = f"{source_urn}:{heuristic_name}"
    else:
        seed = f"{source_urn}:{dest_urn}:{heuristic_name}"
    return str(uuid.uuid5(CANDIDATE_NAMESPACE, seed))


@dataclass
class Candidate:
    """A node suspected of needing investigation.

    For path-linked heuristics (``dest_node_type`` set on the heuristic),
    ``dest_urn``, ``dest_node_type``, ``dest_metadata`` describe the linked
    target node, and ``path`` is the URN sequence from source to dest under
    an undirected shortest-path walk.
    """

    id: str
    source_urn: str
    source_node_type: str
    source_metadata: dict
    heuristic_name: str
    terminal_actions: list[str]
    skill_file: str
    dest_urn: str | None = None
    dest_node_type: str | None = None
    dest_metadata: dict | None = None
    path: list[str] = field(default_factory=list)


@dataclass
class CandidateResult:
    """Outcome of investigating a single candidate."""

    candidate: Candidate
    outcome: str  # "linked", "rejected", "error"
    note: str
    actions: list = field(default_factory=list)  # list[CapturedAction]
    agent_summary: str = ""
    soft_link_id: str | None = None
    links_evaluated: list[dict] | None = None
    worktree_path: str | None = None
    worktree_branch: str | None = None


def filter_already_evaluated(candidates: list[Candidate], store: GraphStore) -> list[Candidate]:
    """Remove candidates whose source node has already been evaluated by its heuristic.

    A node is considered evaluated if its metadata contains
    ``<heuristic_name>_last_evaluated_at``.
    """
    unevaluated: list[Candidate] = []
    with store.lock:
        for c in candidates:
            meta = store.G.nodes[c.source_urn].get("metadata", {})
            if f"{c.heuristic_name}_last_evaluated_at" not in meta:
                unevaluated.append(c)
    return unevaluated
