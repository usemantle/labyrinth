"""Report formatting and persistence for discovery runs."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from src.agent.candidates import CandidateResult

logger = logging.getLogger(__name__)


def format_report(results: list[CandidateResult]) -> str:
    """Format a human-readable summary of discovery results."""
    linked = sum(1 for r in results if r.outcome == "linked")
    rejected = sum(1 for r in results if r.outcome == "rejected")
    errored = sum(1 for r in results if r.outcome == "error")

    lines = [
        "Soft Link Discovery Report",
        f"Candidates investigated: {len(results)}",
        f"  Linked:   {linked}",
        f"  Rejected: {rejected}",
        f"  Errors:   {errored}",
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


def save_report(results: list[CandidateResult], project_dir: Path) -> Path:
    """Write report files to the project's agent_reports directory.

    Returns the path to the text report.
    """
    reports_dir = project_dir / "agent_reports"
    reports_dir.mkdir(exist_ok=True)

    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

    # Text report
    txt_path = reports_dir / f"{timestamp}.txt"
    txt_path.write_text(format_report(results))

    # JSON report
    json_path = reports_dir / f"{timestamp}.json"
    json_data = {
        "timestamp": datetime.now(UTC).isoformat(),
        "total": len(results),
        "linked": sum(1 for r in results if r.outcome == "linked"),
        "rejected": sum(1 for r in results if r.outcome == "rejected"),
        "errors": sum(1 for r in results if r.outcome == "error"),
        "results": [
            {
                "source_urn": r.candidate.source_urn,
                "heuristic": r.candidate.heuristic_name,
                "outcome": r.outcome,
                "soft_link_id": r.soft_link_id,
                "note": r.note,
            }
            for r in results
        ],
    }
    json_path.write_text(json.dumps(json_data, indent=2) + "\n")

    logger.info("Reports saved to %s", reports_dir)
    return txt_path
