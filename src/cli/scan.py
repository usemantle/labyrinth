"""Scan handler — thin wrapper delegating to Scanner."""

from __future__ import annotations

import uuid
from pathlib import Path

from src.graph.scanner import Scanner
from src.graph.sinks.sink import Sink


def run_scan(
    project_name: str,
    project_id: uuid.UUID,
    targets: list[dict],
    sink: Sink,
    project_dir: Path,
    global_config: dict | None = None,
) -> None:
    """Scan all given targets, stitch edges, and write results to the sink."""
    Scanner(project_name, project_id, targets, sink, project_dir, global_config).run()
