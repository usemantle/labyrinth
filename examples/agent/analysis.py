"""Serialize and manage heuristic analysis results (heuristics.json)."""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import asdict
from pathlib import Path

from .candidates import Candidate

logger = logging.getLogger(__name__)


def save_analysis(
    candidates: list[Candidate],
    project_dir: Path,
    graph_generated_at: str,
) -> Path:
    """Atomic-write heuristics.json with the given candidates.

    Returns the path to heuristics.json.
    """
    from datetime import UTC, datetime

    data = {
        "analyzed_at": datetime.now(UTC).isoformat(),
        "graph_generated_at": graph_generated_at,
        "candidates": [
            {**asdict(c), "status": "pending"} for c in candidates
        ],
    }

    heuristics_path = project_dir / "heuristics.json"
    fd, tmp_path = tempfile.mkstemp(dir=project_dir, suffix=".json.tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
            f.write("\n")
        os.replace(tmp_path, heuristics_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    logger.info("Analysis saved to %s", heuristics_path)
    return heuristics_path


def load_analysis(project_dir: Path) -> dict:
    """Read and parse heuristics.json. Raises FileNotFoundError if missing."""
    heuristics_path = project_dir / "heuristics.json"
    if not heuristics_path.exists():
        raise FileNotFoundError(
            f"No heuristics.json found at {heuristics_path}. "
            "Run `labyrinth agent analyze` first."
        )
    return json.loads(heuristics_path.read_text())


def find_candidate(analysis: dict, cid: str) -> Candidate | None:
    """Look up a candidate by UUID in the analysis dict, returning a Candidate or None."""
    for entry in analysis.get("candidates", []):
        if entry["id"] == cid:
            return Candidate(
                id=entry["id"],
                source_urn=entry["source_urn"],
                source_node_type=entry["source_node_type"],
                source_metadata=entry["source_metadata"],
                heuristic_name=entry["heuristic_name"],
                terminal_actions=entry["terminal_actions"],
                skill_file=entry["skill_file"],
            )
    return None


def update_candidate_status(project_dir: Path, cid: str, status: str) -> None:
    """Read heuristics.json, update the status for the given candidate, write back atomically."""
    analysis = load_analysis(project_dir)
    for entry in analysis.get("candidates", []):
        if entry["id"] == cid:
            entry["status"] = status
            break

    heuristics_path = project_dir / "heuristics.json"
    fd, tmp_path = tempfile.mkstemp(dir=project_dir, suffix=".json.tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(analysis, f, indent=2)
            f.write("\n")
        os.replace(tmp_path, heuristics_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
