"""Heuristic registry for soft-link candidate discovery.

To add a new heuristic:
1. Create a new module in this directory with a class inheriting BaseHeuristic
2. Add it to ALL_HEURISTICS below
"""

from __future__ import annotations

from src.agent.candidates import Candidate
from src.agent.heuristics._base import BaseHeuristic
from src.agent.heuristics.orphaned_ecr_repo import OrphanedEcrRepo
from src.agent.heuristics.unlinked_dockerfile import UnlinkedDockerfile
from src.agent.heuristics.unlinked_orm_model import UnlinkedOrmModel
from src.agent.heuristics.unlinked_s3_code import UnlinkedS3Code
from src.mcp.graph_store import GraphStore

ALL_HEURISTICS: list[BaseHeuristic] = [
    UnlinkedDockerfile(),
    UnlinkedS3Code(),
    UnlinkedOrmModel(),
    OrphanedEcrRepo(),
]

HEURISTICS_BY_NAME: dict[str, BaseHeuristic] = {h.name: h for h in ALL_HEURISTICS}


def gather_all_candidates(store: GraphStore) -> list[Candidate]:
    """Run all registered heuristics and return the combined candidate list."""
    candidates: list[Candidate] = []
    for heuristic in ALL_HEURISTICS:
        candidates.extend(heuristic.find(store))
    return candidates


__all__ = [
    "ALL_HEURISTICS",
    "BaseHeuristic",
    "HEURISTICS_BY_NAME",
    "OrphanedEcrRepo",
    "UnlinkedDockerfile",
    "UnlinkedOrmModel",
    "UnlinkedS3Code",
    "gather_all_candidates",
]
