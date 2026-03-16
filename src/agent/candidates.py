"""Candidate data model and check-stage filter."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from src.mcp.graph_store import GraphStore

CANDIDATE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "labyrinth:candidate")


def candidate_id(source_urn: str, heuristic_name: str) -> str:
    """Return a deterministic UUID5 for a (source_urn, heuristic_name) pair."""
    return str(uuid.uuid5(CANDIDATE_NAMESPACE, f"{source_urn}:{heuristic_name}"))


@dataclass
class Candidate:
    """A node suspected of needing investigation."""

    id: str
    source_urn: str
    source_node_type: str
    source_metadata: dict
    heuristic_name: str
    terminal_actions: list[str]
    skill_file: str


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
