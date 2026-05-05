"""Tests for candidate filtering."""

from __future__ import annotations

import json
import tempfile

from labyrinth.agent.candidates import Candidate, candidate_id, filter_already_evaluated
from labyrinth.mcp.graph_store import GraphStore


def _make_candidate(source_urn: str, heuristic_name: str = "test") -> Candidate:
    return Candidate(
        id=candidate_id(source_urn, heuristic_name),
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


class TestCandidateId:
    def test_deterministic(self):
        id1 = candidate_id("urn:a", "test")
        id2 = candidate_id("urn:a", "test")
        assert id1 == id2

    def test_unique_for_different_inputs(self):
        id1 = candidate_id("urn:a", "test")
        id2 = candidate_id("urn:b", "test")
        id3 = candidate_id("urn:a", "other")
        assert id1 != id2
        assert id1 != id3
        assert id2 != id3
