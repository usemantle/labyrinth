"""Tests for candidate filtering."""

from __future__ import annotations

import json
import tempfile

from src.agent.candidates import Candidate, filter_already_evaluated
from src.mcp.graph_store import GraphStore


def _make_candidate(source_urn: str, heuristic_name: str = "test") -> Candidate:
    return Candidate(
        source_urn=source_urn,
        source_node_type="file",
        source_metadata={},
        heuristic_name=heuristic_name,
        terminal_actions=["mark_evaluated"],
        skill_file="",
    )


def _make_store(nodes: list[dict], edges: list[dict] | None = None) -> GraphStore:
    data = {
        "generated_at": "2024-01-01T00:00:00Z",
        "nodes": nodes,
        "edges": edges or [],
        "soft_links": [],
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(data, f)
        path = f.name
    return GraphStore(path)


class TestFilterAlreadyEvaluated:
    def test_keeps_unevaluated(self):
        nodes = [
            {"urn": "urn:a", "node_type": "file", "metadata": {}},
        ]
        store = _make_store(nodes)
        try:
            candidates = [_make_candidate("urn:a")]
            result = filter_already_evaluated(candidates, store)
            assert len(result) == 1
        finally:
            store.stop_watcher()

    def test_removes_evaluated(self):
        nodes = [
            {"urn": "urn:a", "node_type": "file", "metadata": {"test_last_evaluated_at": "2024-01-01"}},
        ]
        store = _make_store(nodes)
        try:
            candidates = [_make_candidate("urn:a", "test")]
            result = filter_already_evaluated(candidates, store)
            assert len(result) == 0
        finally:
            store.stop_watcher()

    def test_different_heuristic_not_filtered(self):
        nodes = [
            {"urn": "urn:a", "node_type": "file", "metadata": {"other_last_evaluated_at": "2024-01-01"}},
        ]
        store = _make_store(nodes)
        try:
            candidates = [_make_candidate("urn:a", "test")]
            result = filter_already_evaluated(candidates, store)
            assert len(result) == 1
        finally:
            store.stop_watcher()

    def test_empty_candidates(self):
        store = _make_store([])
        try:
            assert filter_already_evaluated([], store) == []
        finally:
            store.stop_watcher()
