"""Tests for report formatting and persistence."""

from __future__ import annotations

import json

from labyrinth.agent.candidates import Candidate, CandidateResult, candidate_id
from labyrinth.agent.report import format_report, save_report


def _make_result(outcome: str, note: str = "", soft_link_id: str | None = None) -> CandidateResult:
    return CandidateResult(
        candidate=Candidate(
            id=candidate_id(f"urn:test:{outcome}", "test"),
            source_urn=f"urn:test:{outcome}",
            source_node_type="file",
            source_metadata={},
            heuristic_name="test",
            terminal_actions=["mark_evaluated"],
            skill_file="",
        ),
        outcome=outcome,
        note=note,
        soft_link_id=soft_link_id,
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
    def test_creates_reports_json(self, tmp_path):
        results = [
            _make_result("linked", "found it", "id-1"),
            _make_result("rejected"),
        ]
        reports_path = save_report(results, tmp_path, run_id="test-run-id", started_at="2024-01-01T00:00:00Z")

        assert reports_path.exists()
        assert reports_path.name == "reports.json"

        data = json.loads(reports_path.read_text())
        assert len(data["runs"]) == 1

        run = data["runs"][0]
        assert run["run_id"] == "test-run-id"
        assert run["summary"]["total_candidates"] == 2
        assert run["summary"]["linked"] == 1
        assert run["summary"]["rejected"] == 1
        assert len(run["results"]) == 2

    def test_appends_to_existing(self, tmp_path):
        results = [_make_result("linked", "first", "id-1")]
        save_report(results, tmp_path, run_id="run-1", started_at="2024-01-01T00:00:00Z")

        results2 = [_make_result("rejected", "second")]
        save_report(results2, tmp_path, run_id="run-2", started_at="2024-01-02T00:00:00Z")

        data = json.loads((tmp_path / "reports.json").read_text())
        assert len(data["runs"]) == 2
        assert data["runs"][0]["run_id"] == "run-1"
        assert data["runs"][1]["run_id"] == "run-2"
