"""Tests for the analysis serialization layer."""

from __future__ import annotations

import json

from labyrinth.agent.analysis import find_candidate, load_analysis, save_analysis, update_candidate_status
from labyrinth.agent.candidates import Candidate, candidate_id


def _make_candidate(source_urn: str, heuristic_name: str = "test") -> Candidate:
    return Candidate(
        id=candidate_id(source_urn, heuristic_name),
        source_urn=source_urn,
        source_node_type="file",
        source_metadata={"key": "value"},
        heuristic_name=heuristic_name,
        terminal_actions=["mark_evaluated"],
        skill_file="",
    )


class TestSaveAnalysis:
    def test_creates_valid_json(self, tmp_path):
        candidates = [_make_candidate("urn:a"), _make_candidate("urn:b")]
        path = save_analysis(candidates, tmp_path, "2024-01-01T00:00:00Z")

        assert path.exists()
        assert path.name == "heuristics.json"

        data = json.loads(path.read_text())
        assert data["graph_generated_at"] == "2024-01-01T00:00:00Z"
        assert data["analyzed_at"] is not None
        assert len(data["candidates"]) == 2
        assert data["candidates"][0]["status"] == "pending"
        assert data["candidates"][0]["id"] == candidate_id("urn:a", "test")


class TestLoadAnalysis:
    def test_round_trips(self, tmp_path):
        candidates = [_make_candidate("urn:a")]
        save_analysis(candidates, tmp_path, "2024-01-01T00:00:00Z")

        analysis = load_analysis(tmp_path)
        assert len(analysis["candidates"]) == 1
        assert analysis["candidates"][0]["source_urn"] == "urn:a"

    def test_raises_if_missing(self, tmp_path):
        import pytest

        with pytest.raises(FileNotFoundError, match="heuristics.json"):
            load_analysis(tmp_path)


class TestFindCandidate:
    def test_finds_by_uuid(self, tmp_path):
        candidates = [_make_candidate("urn:a"), _make_candidate("urn:b")]
        save_analysis(candidates, tmp_path, "2024-01-01T00:00:00Z")
        analysis = load_analysis(tmp_path)

        cid = candidate_id("urn:a", "test")
        result = find_candidate(analysis, cid)
        assert result is not None
        assert result.source_urn == "urn:a"
        assert result.id == cid

    def test_returns_none_for_unknown(self, tmp_path):
        candidates = [_make_candidate("urn:a")]
        save_analysis(candidates, tmp_path, "2024-01-01T00:00:00Z")
        analysis = load_analysis(tmp_path)

        result = find_candidate(analysis, "nonexistent-uuid")
        assert result is None


class TestUpdateCandidateStatus:
    def test_changes_status(self, tmp_path):
        candidates = [_make_candidate("urn:a")]
        save_analysis(candidates, tmp_path, "2024-01-01T00:00:00Z")

        cid = candidate_id("urn:a", "test")
        update_candidate_status(tmp_path, cid, "running")

        analysis = load_analysis(tmp_path)
        assert analysis["candidates"][0]["status"] == "running"

        update_candidate_status(tmp_path, cid, "completed")
        analysis = load_analysis(tmp_path)
        assert analysis["candidates"][0]["status"] == "completed"
