"""Pipeline orchestrator: analyze → run single candidate."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path

from labyrinth.agent.analysis import find_candidate, load_analysis, save_analysis, update_candidate_status
from labyrinth.agent.candidates import Candidate, CandidateResult, filter_already_evaluated
from labyrinth.agent.emitter import emit_candidate
from labyrinth.agent.heuristics import gather_all_candidates
from labyrinth.agent.report import format_report, save_report
from labyrinth.mcp.graph_store import GraphStore

logger = logging.getLogger(__name__)


async def run_analysis(project_dir: Path) -> list[Candidate]:
    """Evaluate all heuristics and save findings to heuristics.json.

    1. Load graph
    2. Gather all candidates
    3. Filter already-evaluated
    4. Save to heuristics.json
    5. Print summary
    """
    graph_path = project_dir / "graph.json"
    store = GraphStore(str(graph_path))

    try:
        candidates = gather_all_candidates(store)
        logger.info("Gathered %d raw candidates", len(candidates))

        candidates = filter_already_evaluated(candidates, store)
        logger.info("After filtering: %d candidates", len(candidates))

        analysis_path = save_analysis(candidates, project_dir, store.generated_at)

        # Print summary
        if not candidates:
            print("No candidates found.")
        else:
            # Count per heuristic
            by_heuristic: dict[str, int] = {}
            for c in candidates:
                by_heuristic[c.heuristic_name] = by_heuristic.get(c.heuristic_name, 0) + 1

            print(f"Analysis complete: {len(candidates)} candidate(s)\n")
            for name, count in sorted(by_heuristic.items()):
                print(f"  {name}: {count}")
            print(f"\nSaved to {analysis_path}")
            print("\nCandidates:")
            for c in candidates:
                print(f"  {c.id[:12]}  [{c.heuristic_name}] {c.source_urn}")

        return candidates
    finally:
        store.stop_watcher()


async def run_single_candidate(project_dir: Path, candidate_id: str) -> CandidateResult:
    """Execute the agent against a single candidate by UUID.

    1. Load analysis
    2. Find candidate
    3. Validate graph freshness
    4. Run agent
    5. Save report + update status
    """
    # 1. Load analysis
    try:
        analysis = load_analysis(project_dir)
    except FileNotFoundError as exc:
        raise SystemExit(str(exc)) from None

    # 2. Find candidate
    candidate = find_candidate(analysis, candidate_id)
    if candidate is None:
        lines = [f"Candidate '{candidate_id}' not found.\n", "Available candidates:"]
        for entry in analysis.get("candidates", []):
            lines.append(f"  {entry['id'][:12]}  [{entry['heuristic_name']}] {entry['source_urn']}")
        raise SystemExit("\n".join(lines))

    # 3. Load graph and validate
    graph_path = project_dir / "graph.json"
    store = GraphStore(str(graph_path))

    try:
        if store.generated_at != analysis.get("graph_generated_at"):
            logger.warning(
                "Graph has changed since analysis (graph: %s, analysis: %s). "
                "Consider re-running `labyrinth agent analyze`.",
                store.generated_at,
                analysis.get("graph_generated_at"),
            )

        # Validate candidate URN exists in graph
        with store.lock:
            if candidate.source_urn not in store.G.nodes:
                raise SystemExit(
                    f"Candidate URN '{candidate.source_urn}' no longer exists in the graph. "
                    "Re-run `labyrinth agent analyze`."
                )

        # 4. Update status to running
        update_candidate_status(project_dir, candidate_id, "running")

        # 5. Run agent
        run_id = str(uuid.uuid4())
        started_at = datetime.now(UTC).isoformat()

        result = await emit_candidate(candidate, store, project_dir)

        # 6. Save report
        report_path = save_report([result], project_dir, run_id=run_id, started_at=started_at)
        print(format_report([result]))
        print(f"\nFull report saved to {report_path}")

        # 7. Update status
        status = "error" if result.outcome == "error" else "completed"
        update_candidate_status(project_dir, candidate_id, status)

        return result
    finally:
        store.stop_watcher()
