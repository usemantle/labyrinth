"""Tests for report formatting and persistence."""

from __future__ import annotations

import json

from src.agent.candidates import Candidate, CandidateResult
from src.agent.report import format_report, save_report


def _make_result(outcome: str, note: str = "", soft_link_id: str | None = None) -> CandidateResult:
    return CandidateResult(
        candidate=Candidate(
            source_urn=f"urn:test:{outcome}",
            source_node_type="file",
            source_metadata={},
            heuristic_name="test",
            target_edge_type="builds",
            target_node_type="image_repository",
            skill_file="",
        ),
        outcome=outcome,
        soft_link_id=soft_link_id,
        note=note,
    )


class TestFormatReport:
    def test_counts_are_correct(self):
        results = [
            _make_result("linked", "matched", "id-1"),
            _make_result("linked", "matched", "id-2"),
            _make_result("rejected"),
            _make_result("error", "boom"),
        ]
        report = format_report(results)
        assert "Candidates investigated: 4" in report
        assert "Linked:   2" in report
        assert "Rejected: 1" in report
        assert "Errors:   1" in report

    def test_empty_results(self):
        report = format_report([])
        assert "Candidates investigated: 0" in report
        assert "Linked:   0" in report

    def test_linked_details_shown(self):
        results = [_make_result("linked", "evidence here", "link-123")]
        report = format_report(results)
        assert "link-123" in report
        assert "evidence here" in report

    def test_error_details_shown(self):
        results = [_make_result("error", "something broke")]
        report = format_report(results)
        assert "something broke" in report

    def test_rejected_reasoning_shown(self):
        results = [_make_result("rejected", "No matching S3 bucket found in graph")]
        report = format_report(results)
        assert "Rejected:" in report
        assert "No matching S3 bucket found in graph" in report
        assert "urn:test:rejected" in report


class TestSaveReport:
    def test_creates_files(self, tmp_path):
        results = [
            _make_result("linked", "found it", "id-1"),
            _make_result("rejected"),
        ]
        txt_path = save_report(results, tmp_path)

        assert txt_path.exists()
        assert txt_path.suffix == ".txt"
        assert "Candidates investigated: 2" in txt_path.read_text()

        # JSON file should be alongside the txt
        json_files = list((tmp_path / "agent_reports").glob("*.json"))
        assert len(json_files) == 1

        data = json.loads(json_files[0].read_text())
        assert data["total"] == 2
        assert data["linked"] == 1
        assert data["rejected"] == 1
        assert len(data["results"]) == 2
