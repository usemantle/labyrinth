"""Report formatting and persistence for discovery runs."""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from src.agent.candidates import CandidateResult

logger = logging.getLogger(__name__)


def format_report(results: list[CandidateResult]) -> str:
    """Format a human-readable summary of discovery results."""
    linked = sum(1 for r in results if r.outcome == "linked")
    rejected = sum(1 for r in results if r.outcome == "rejected")
    errored = sum(1 for r in results if r.outcome == "error")
    total_actions = sum(len(r.actions) for r in results)

    lines = [
        "Soft Link Discovery Report",
        f"Candidates investigated: {len(results)}",
        f"  Linked:   {linked}",
        f"  Rejected: {rejected}",
        f"  Errors:   {errored}",
        f"  Actions:  {total_actions}",
    ]

    if any(r.outcome == "linked" for r in results):
        lines.append("\nLinked:")
        for r in results:
            if r.outcome == "linked":
                lines.append(f"  {r.candidate.source_urn}")
                lines.append(f"    → soft_link_id={r.soft_link_id}")
                lines.append(f"    → {r.note[:120]}")

    if any(r.outcome == "rejected" for r in results):
        lines.append("\nRejected:")
        for r in results:
            if r.outcome == "rejected":
                lines.append(f"  {r.candidate.source_urn}")
                lines.append(f"    Reason: {r.note}")

    if any(r.outcome == "error" for r in results):
        lines.append("\nErrors:")
        for r in results:
            if r.outcome == "error":
                lines.append(f"  {r.candidate.source_urn}: {r.note}")

    return "\n".join(lines)


def _serialize_result(r: CandidateResult) -> dict:
    """Convert a CandidateResult to the JSON dict shape for reports.json."""
    actions = []
    for a in r.actions:
        actions.append(asdict(a))

    return {
        "candidate_urn": r.candidate.source_urn,
        "candidate_node_type": r.candidate.source_node_type,
        "heuristic_name": r.candidate.heuristic_name,
        "outcome": r.outcome,
        "agent_summary": r.agent_summary,
        "actions": actions,
        "links_evaluated": r.links_evaluated or [],
        "soft_link_id": r.soft_link_id,
    }


def save_report(
    results: list[CandidateResult],
    project_dir: Path,
    run_id: str,
    started_at: str,
) -> Path:
    """Write/append a structured run entry to reports.json.

    Returns the path to reports.json.
    """
    reports_path = project_dir / "reports.json"

    # Read existing or init
    if reports_path.exists():
        try:
            data = json.loads(reports_path.read_text())
        except (json.JSONDecodeError, OSError):
            data = {"runs": []}
    else:
        data = {"runs": []}

    linked = sum(1 for r in results if r.outcome == "linked")
    rejected = sum(1 for r in results if r.outcome == "rejected")
    errors = sum(1 for r in results if r.outcome == "error")

    # Collect unique heuristic names
    heuristics_run = sorted({r.candidate.heuristic_name for r in results})

    run_entry = {
        "run_id": run_id,
        "started_at": started_at,
        "completed_at": datetime.now(UTC).isoformat(),
        "heuristics_run": heuristics_run,
        "summary": {
            "total_candidates": len(results),
            "linked": linked,
            "rejected": rejected,
            "errors": errors,
        },
        "results": [_serialize_result(r) for r in results],
    }

    data["runs"].append(run_entry)

    # Atomic write via temp file + os.replace
    fd, tmp_path = tempfile.mkstemp(dir=project_dir, suffix=".json.tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
            f.write("\n")
        os.replace(tmp_path, reports_path)
    except Exception:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    logger.info("Report saved to %s", reports_path)
    return reports_path
