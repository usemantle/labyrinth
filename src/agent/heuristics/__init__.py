"""Heuristic registry for candidate discovery.

To add a new heuristic:
1. Create a new module in this directory with a class inheriting BaseHeuristic
2. Add it to ALL_HEURISTICS below
"""

from __future__ import annotations

from src.agent.candidates import Candidate
from src.agent.heuristics._base import BaseHeuristic, TerminalAction
from src.agent.heuristics.configurable import ConfigurableHeuristic
from src.agent.heuristics.insecure_endpoint import InsecureEndpoint
from src.agent.heuristics.orphaned_ecr_repo import OrphanedEcrRepo
from src.agent.heuristics.unlinked_dockerfile import UnlinkedDockerfile
from src.agent.heuristics.unlinked_entrypoint import UnlinkedEntrypoint
from src.agent.heuristics.vulnerable_dependency import VulnerableDependency
from src.mcp.graph_store import GraphStore

ALL_HEURISTICS: list[BaseHeuristic] = [
    UnlinkedDockerfile(),
    OrphanedEcrRepo(),
    InsecureEndpoint(),
    VulnerableDependency(),
    UnlinkedEntrypoint(),
]

HEURISTICS_BY_NAME: dict[str, BaseHeuristic] = {h.name: h for h in ALL_HEURISTICS}


def gather_all_candidates(
    store: GraphStore,
    heuristic_names: list[str] | None = None,
    extra_heuristics: list[BaseHeuristic] | None = None,
) -> list[Candidate]:
    """Run registered heuristics and return the combined candidate list.

    If *heuristic_names* is given, only run heuristics whose ``name``
    is in the list.  Pass ``None`` (the default) to run all.

    *extra_heuristics* are appended after ``ALL_HEURISTICS`` and subject
    to the same name filter.
    """
    heuristics: list[BaseHeuristic] = list(ALL_HEURISTICS) + (extra_heuristics or [])
    if heuristic_names is not None:
        heuristics = [h for h in heuristics if h.name in heuristic_names]
    candidates: list[Candidate] = []
    for heuristic in heuristics:
        candidates.extend(heuristic.find(store))
    return candidates


__all__ = [
    "ALL_HEURISTICS",
    "BaseHeuristic",
    "ConfigurableHeuristic",
    "HEURISTICS_BY_NAME",
    "InsecureEndpoint",
    "OrphanedEcrRepo",
    "TerminalAction",
    "UnlinkedDockerfile",
    "UnlinkedEntrypoint",
    "VulnerableDependency",
    "gather_all_candidates",
]
