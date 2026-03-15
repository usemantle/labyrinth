"""Tests for prompt construction."""

from __future__ import annotations

import json
import tempfile

from src.agent.candidates import Candidate
from src.agent.prompts import build_investigation_prompt, build_system_prompt
from src.mcp.graph_store import GraphStore


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


class TestBuildSystemPrompt:
    def test_contains_confidence_guidelines(self):
        prompt = build_system_prompt()
        assert "VERY_HIGH" in prompt
        assert "HIGH" in prompt
        assert "MEDIUM" in prompt
        assert "LOW" in prompt

    def test_contains_rules(self):
        prompt = build_system_prompt()
        assert "add_soft_link" in prompt
        assert "evidence" in prompt


class TestBuildInvestigationPrompt:
    def test_contains_candidate_urn(self):
        candidate = Candidate(
            source_urn="urn:github:repo:org:::org/app/Dockerfile",
            source_node_type="file",
            source_metadata={"dockerfile_base_images": ["python:3.12"]},
            heuristic_name="unlinked_dockerfile",
            output_type="soft_link",
            skill_file="",
        )
        ecr_node = {
            "urn": "urn:aws:ecr:123:us-east-1:app",
            "node_type": "image_repository",
            "metadata": {},
        }
        dockerfile_node = {
            "urn": candidate.source_urn,
            "node_type": "file",
            "metadata": {"dockerfile_base_images": ["python:3.12"]},
        }
        store = _make_store([dockerfile_node, ecr_node])
        try:
            prompt = build_investigation_prompt(candidate, store)
            assert candidate.source_urn in prompt
            assert "unlinked_dockerfile" in prompt
        finally:
            store.stop_watcher()

    def test_includes_task_instruction(self):
        candidate = Candidate(
            source_urn="urn:test",
            source_node_type="function",
            source_metadata={"aws_s3_operations": ["put_object"]},
            heuristic_name="unlinked_s3_code",
            output_type="soft_link",
            skill_file="",
        )
        node = {"urn": "urn:test", "node_type": "function", "metadata": {}}
        store = _make_store([node])
        try:
            prompt = build_investigation_prompt(candidate, store)
            assert "S3 operations" in prompt
        finally:
            store.stop_watcher()

    def test_soft_link_output_includes_instructions(self):
        candidate = Candidate(
            source_urn="urn:test",
            source_node_type="file",
            source_metadata={},
            heuristic_name="unlinked_dockerfile",
            output_type="soft_link",
            skill_file="",
        )
        node = {"urn": "urn:test", "node_type": "file", "metadata": {}}
        store = _make_store([node])
        try:
            prompt = build_investigation_prompt(candidate, store)
            assert "add_soft_link" in prompt
            assert "update_node_metadata" in prompt
        finally:
            store.stop_watcher()

    def test_embeds_skill_file(self):
        candidate = Candidate(
            source_urn="urn:test",
            source_node_type="file",
            source_metadata={},
            heuristic_name="unlinked_dockerfile",
            output_type="soft_link",
            skill_file="link-dockerfile-to-ecr.md",
        )
        node = {"urn": "urn:test", "node_type": "file", "metadata": {}}
        store = _make_store([node])
        try:
            prompt = build_investigation_prompt(candidate, store)
            assert "Investigation playbook" in prompt
            assert "Link Dockerfile to ECR" in prompt
        finally:
            store.stop_watcher()
