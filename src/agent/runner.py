"""Pipeline orchestrator: gather → check → emit → report."""

from __future__ import annotations

import logging
from pathlib import Path

from src.agent.candidates import CandidateResult, filter_already_evaluated
from src.agent.emitter import emit_all
from src.agent.heuristics import gather_all_candidates
from src.agent.report import format_report, save_report
from src.mcp.graph_store import GraphStore

logger = logging.getLogger(__name__)


def print_candidates(candidates: list) -> None:
    """Pretty-print the candidate list for dry-run mode."""
    if not candidates:
        print("No candidates found.")
        return

    print(f"Candidates ({len(candidates)}):\n")
    for i, c in enumerate(candidates, 1):
        print(f"  {i}. [{c.heuristic_name}] {c.source_urn}")
        print(f"     output_type: {c.output_type}")
        if c.skill_file:
            print(f"     skill:  {c.skill_file}")
        print()


async def run_discovery(
    project_dir: Path,
    *,
    dry_run: bool = False,
    heuristic_names: list[str] | None = None,
) -> list[CandidateResult]:
    """Run the full discovery pipeline.

    1. Load the graph
    2. Gather candidates via heuristics
    3. Filter out already-evaluated candidates
    4. (dry_run) Print candidates and return
    5. Emit: invoke Claude agent for each candidate
    6. Save report
    """
    graph_path = project_dir / "graph.json"
    store = GraphStore(str(graph_path))

    try:
        candidates = gather_all_candidates(store, heuristic_names=heuristic_names)
        logger.info("Gathered %d raw candidates", len(candidates))

        candidates = filter_already_evaluated(candidates, store)
        logger.info("After filtering: %d candidates", len(candidates))

        if dry_run:
            print_candidates(candidates)
            return []

        if not candidates:
            print("No candidates to investigate.")
            return []

        results = await emit_all(candidates, store, project_dir)

        report_path = save_report(results, project_dir)
        print(format_report(results))
        print(f"\nFull report saved to {report_path}")

        return results
    finally:
        store.stop_watcher()
