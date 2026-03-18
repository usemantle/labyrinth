"""Tests for deterministic heuristics that query the graph."""

from __future__ import annotations

import json
import tempfile

import pytest

from src.agent.candidates import candidate_id
from src.agent.heuristics import (
    ConfigurableHeuristic,
    InsecureEndpoint,
    OrphanedEcrRepo,
    UnlinkedDockerfile,
    UnlinkedEntrypoint,
    UnlinkedS3Code,
    VulnerableDependency,
    gather_all_candidates,
)
from src.agent.heuristics._base import TerminalAction
from src.mcp.graph_store import GraphStore


def _make_store(nodes: list[dict], edges: list[dict] | None = None) -> GraphStore:
    """Create a GraphStore from synthetic graph data."""
    data = {
        "generated_at": "2024-01-01T00:00:00Z",
        "nodes": nodes,
        "edges": edges or [],
        "soft_links": [],
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(data, f)
        path = f.name

    store = GraphStore(path)
    return store


@pytest.fixture
def dockerfile_node():
    return {
        "urn": "urn:github:repo:org:::org/app/Dockerfile",
        "node_type": "file",
        "metadata": {
            "file_path": "Dockerfile",
            "dockerfile_base_images": ["python:3.12-slim"],
        },
    }


@pytest.fixture
def ecr_node():
    return {
        "urn": "urn:aws:ecr:123456789:us-east-1:app-image",
        "node_type": "image_repository",
        "metadata": {"repository_name": "app-image"},
    }


@pytest.fixture
def function_with_s3():
    return {
        "urn": "urn:github:repo:org:::org/app/src/upload.py::upload_file",
        "node_type": "function",
        "metadata": {
            "function_name": "upload_file",
            "aws_s3_operations": ["put_object"],
        },
    }


class TestUnlinkedDockerfile:
    heuristic = UnlinkedDockerfile()

    def test_returns_dockerfile_without_builds_edge(self, dockerfile_node, ecr_node):
        store = _make_store([dockerfile_node, ecr_node])
        try:
            candidates = self.heuristic.find(store)
            assert len(candidates) == 1
            assert candidates[0].source_urn == dockerfile_node["urn"]
            assert candidates[0].heuristic_name == "unlinked_dockerfile"
            assert candidates[0].id == candidate_id(dockerfile_node["urn"], "unlinked_dockerfile")
        finally:
            store.stop_watcher()

    def test_ignores_non_dockerfile_files(self):
        node = {
            "urn": "urn:github:repo:org:::org/app/src/main.py",
            "node_type": "file",
            "metadata": {"file_path": "src/main.py", "language": "python"},
        }
        store = _make_store([node])
        try:
            assert self.heuristic.find(store) == []
        finally:
            store.stop_watcher()

    def test_get_instructions(self):
        instructions = self.heuristic.get_instructions()
        assert "Dockerfile" in instructions
        assert "builds" in instructions

    def test_terminal_actions(self):
        assert self.heuristic.terminal_actions == [
            TerminalAction.MARK_EVALUATED,
            TerminalAction.CREATE_SOFT_LINK,
        ]


class TestUnlinkedS3Code:
    heuristic = UnlinkedS3Code()

    def test_returns_function_with_s3_ops(self, function_with_s3):
        store = _make_store([function_with_s3])
        try:
            candidates = self.heuristic.find(store)
            assert len(candidates) == 1
            assert candidates[0].heuristic_name == "unlinked_s3_code"
        finally:
            store.stop_watcher()

    def test_ignores_functions_without_s3(self):
        node = {
            "urn": "urn:github:repo:org:::org/app/src/main.py::main",
            "node_type": "function",
            "metadata": {"function_name": "main"},
        }
        store = _make_store([node])
        try:
            assert self.heuristic.find(store) == []
        finally:
            store.stop_watcher()

    def test_get_instructions(self):
        instructions = self.heuristic.get_instructions()
        assert "S3" in instructions
        assert "aws_s3_operations" in instructions

    def test_terminal_actions(self):
        assert self.heuristic.terminal_actions == [
            TerminalAction.MARK_EVALUATED,
            TerminalAction.CREATE_SOFT_LINK,
        ]


class TestOrphanedEcrRepo:
    heuristic = OrphanedEcrRepo()

    def test_returns_ecr_repo(self, ecr_node):
        store = _make_store([ecr_node])
        try:
            candidates = self.heuristic.find(store)
            assert len(candidates) == 1
            assert candidates[0].heuristic_name == "orphaned_ecr_repo"
        finally:
            store.stop_watcher()

    def test_no_ecr_repos(self):
        store = _make_store([])
        try:
            assert self.heuristic.find(store) == []
        finally:
            store.stop_watcher()

    def test_get_instructions(self):
        instructions = self.heuristic.get_instructions()
        assert "ECR" in instructions
        assert "builds" in instructions

    def test_terminal_actions(self):
        assert self.heuristic.terminal_actions == [
            TerminalAction.MARK_EVALUATED,
            TerminalAction.CREATE_SOFT_LINK,
        ]


class TestInsecureEndpoint:
    heuristic = InsecureEndpoint()

    def test_returns_unauthenticated_endpoint(self):
        node = {
            "urn": "urn:github:repo:org:::org/app/src/main.py::get_file",
            "node_type": "function",
            "metadata": {
                "function_name": "get_file",
                "http_method": "GET",
                "route_path": "/files/{file_path}",
            },
        }
        store = _make_store([node])
        try:
            candidates = self.heuristic.find(store)
            assert len(candidates) == 1
            assert candidates[0].heuristic_name == "insecure_endpoint"
            assert "create_pr" in candidates[0].terminal_actions
            assert candidates[0].id == candidate_id(node["urn"], "insecure_endpoint")
        finally:
            store.stop_watcher()

    def test_ignores_authenticated_endpoint(self):
        node = {
            "urn": "urn:github:repo:org:::org/app/src/main.py::get_file",
            "node_type": "function",
            "metadata": {
                "function_name": "get_file",
                "http_method": "GET",
                "route_path": "/files/{file_path}",
                "auth_scheme": "oauth2",
            },
        }
        store = _make_store([node])
        try:
            assert self.heuristic.find(store) == []
        finally:
            store.stop_watcher()

    def test_ignores_non_http_function(self):
        node = {
            "urn": "urn:github:repo:org:::org/app/src/utils.py::helper",
            "node_type": "function",
            "metadata": {"function_name": "helper"},
        }
        store = _make_store([node])
        try:
            assert self.heuristic.find(store) == []
        finally:
            store.stop_watcher()

    def test_terminal_actions(self):
        assert self.heuristic.terminal_actions == [
            TerminalAction.MARK_EVALUATED,
            TerminalAction.CREATE_PR,
        ]


class TestVulnerableDependency:
    heuristic = VulnerableDependency()

    def test_returns_dependency_with_cves(self):
        node = {
            "urn": "urn:github:repo:org:::org/app/aiohttp",
            "node_type": "dependency",
            "metadata": {
                "package_name": "aiohttp",
                "package_version": "3.9.1",
                "cve_ids": ["CVE-2024-23334"],
            },
        }
        store = _make_store([node])
        try:
            candidates = self.heuristic.find(store)
            assert len(candidates) == 1
            assert candidates[0].heuristic_name == "vulnerable_dependency"
            assert "create_pr" in candidates[0].terminal_actions
        finally:
            store.stop_watcher()

    def test_ignores_clean_dependency(self):
        node = {
            "urn": "urn:github:repo:org:::org/app/fastapi",
            "node_type": "dependency",
            "metadata": {
                "package_name": "fastapi",
                "package_version": "0.115.0",
            },
        }
        store = _make_store([node])
        try:
            assert self.heuristic.find(store) == []
        finally:
            store.stop_watcher()

    def test_terminal_actions(self):
        assert self.heuristic.terminal_actions == [
            TerminalAction.MARK_EVALUATED,
            TerminalAction.CREATE_PR,
        ]


class TestUnlinkedEntrypoint:
    heuristic = UnlinkedEntrypoint()

    def test_returns_dockerfile_with_cmd_no_executes_edge(self):
        node = {
            "urn": "urn:github:repo:org:::org/app/Dockerfile",
            "node_type": "file",
            "metadata": {
                "file_path": "Dockerfile",
                "dockerfile_base_images": "python:3.12",
                "dockerfile_cmd": '["python", "src/main.py"]',
            },
        }
        store = _make_store([node])
        try:
            candidates = self.heuristic.find(store)
            assert len(candidates) == 1
            assert candidates[0].heuristic_name == "unlinked_entrypoint"
        finally:
            store.stop_watcher()

    def test_returns_dockerfile_with_entrypoint_no_executes_edge(self):
        node = {
            "urn": "urn:github:repo:org:::org/app/Dockerfile",
            "node_type": "file",
            "metadata": {
                "file_path": "Dockerfile",
                "dockerfile_base_images": "python:3.12",
                "dockerfile_entrypoint": '["python", "src/main.py"]',
            },
        }
        store = _make_store([node])
        try:
            candidates = self.heuristic.find(store)
            assert len(candidates) == 1
            assert candidates[0].heuristic_name == "unlinked_entrypoint"
        finally:
            store.stop_watcher()

    def test_ignores_dockerfile_with_executes_edge(self):
        node = {
            "urn": "urn:github:repo:org:::org/app/Dockerfile",
            "node_type": "file",
            "metadata": {
                "file_path": "Dockerfile",
                "dockerfile_base_images": "python:3.12",
                "dockerfile_cmd": '["python", "src/main.py"]',
            },
        }
        target = {
            "urn": "urn:github:repo:org:::org/app/src/main.py",
            "node_type": "file",
            "metadata": {"file_path": "src/main.py"},
        }
        edge = {
            "uuid": "aaaaaaaa-bbbb-cccc-dddd-ffffffffffff",
            "from_urn": node["urn"],
            "to_urn": target["urn"],
            "edge_type": "executes",
            "metadata": {},
        }
        store = _make_store([node, target], [edge])
        try:
            candidates = self.heuristic.find(store)
            assert len(candidates) == 0
        finally:
            store.stop_watcher()

    def test_ignores_file_without_entrypoint_metadata(self):
        node = {
            "urn": "urn:github:repo:org:::org/app/src/main.py",
            "node_type": "file",
            "metadata": {"file_path": "src/main.py", "language": "python"},
        }
        store = _make_store([node])
        try:
            assert self.heuristic.find(store) == []
        finally:
            store.stop_watcher()

    def test_get_instructions(self):
        instructions = self.heuristic.get_instructions()
        assert "ENTRYPOINT" in instructions
        assert "executes" in instructions

    def test_terminal_actions(self):
        assert self.heuristic.terminal_actions == [
            TerminalAction.MARK_EVALUATED,
            TerminalAction.CREATE_SOFT_LINK,
        ]


class TestGatherAll:
    def test_combines_all_heuristics(self, dockerfile_node, ecr_node, function_with_s3):
        # Add nodes for the new heuristics too
        unauthenticated_endpoint = {
            "urn": "urn:github:repo:org:::org/app/src/main.py::get_file",
            "node_type": "function",
            "metadata": {
                "function_name": "get_file",
                "http_method": "GET",
                "route_path": "/files/{file_path}",
            },
        }
        vulnerable_dep = {
            "urn": "urn:github:repo:org:::org/app/aiohttp",
            "node_type": "dependency",
            "metadata": {
                "package_name": "aiohttp",
                "package_version": "3.9.1",
                "cve_ids": ["CVE-2024-23334"],
            },
        }
        # The dockerfile_node fixture already has dockerfile_base_images.
        # Add entrypoint metadata so unlinked_entrypoint picks it up too.
        dockerfile_with_cmd = dict(dockerfile_node)
        dockerfile_with_cmd["metadata"] = {
            **dockerfile_node["metadata"],
            "dockerfile_cmd": '["python", "app.py"]',
        }
        store = _make_store([
            dockerfile_with_cmd, ecr_node, function_with_s3,
            unauthenticated_endpoint, vulnerable_dep,
        ])
        try:
            candidates = gather_all_candidates(store)
            heuristic_names = {c.heuristic_name for c in candidates}
            assert "unlinked_dockerfile" in heuristic_names
            assert "unlinked_s3_code" in heuristic_names
            assert "orphaned_ecr_repo" in heuristic_names
            assert "insecure_endpoint" in heuristic_names
            assert "vulnerable_dependency" in heuristic_names
            assert "unlinked_entrypoint" in heuristic_names
        finally:
            store.stop_watcher()

    def test_extra_heuristics_included(self):
        node = {
            "urn": "urn:github:repo:org:::org/app/src/main.py::my_func",
            "node_type": "function",
            "metadata": {"custom_flag": True},
        }
        extra = ConfigurableHeuristic(
            name="my_custom",
            source_node_type="function",
            metadata_keys=["custom_flag"],
            terminal_actions=[TerminalAction.MARK_EVALUATED],
        )
        store = _make_store([node])
        try:
            candidates = gather_all_candidates(store, extra_heuristics=[extra])
            names = {c.heuristic_name for c in candidates}
            assert "my_custom" in names
        finally:
            store.stop_watcher()


class TestConfigurableHeuristic:
    def test_or_logic_matches_any_key(self):
        node = {
            "urn": "urn:test:node:1",
            "node_type": "function",
            "metadata": {"key_a": "val"},
        }
        h = ConfigurableHeuristic(
            name="test_or",
            source_node_type="function",
            metadata_keys=["key_a", "key_b"],
            terminal_actions=[TerminalAction.MARK_EVALUATED],
            metadata_key_op="OR",
        )
        store = _make_store([node])
        try:
            assert len(h.find(store)) == 1
        finally:
            store.stop_watcher()

    def test_or_logic_misses_no_keys(self):
        node = {
            "urn": "urn:test:node:1",
            "node_type": "function",
            "metadata": {"other_key": "val"},
        }
        h = ConfigurableHeuristic(
            name="test_or",
            source_node_type="function",
            metadata_keys=["key_a", "key_b"],
            terminal_actions=[TerminalAction.MARK_EVALUATED],
            metadata_key_op="OR",
        )
        store = _make_store([node])
        try:
            assert h.find(store) == []
        finally:
            store.stop_watcher()

    def test_and_logic_requires_all_keys(self):
        node_both = {
            "urn": "urn:test:node:both",
            "node_type": "function",
            "metadata": {"key_a": "val", "key_b": "val"},
        }
        node_one = {
            "urn": "urn:test:node:one",
            "node_type": "function",
            "metadata": {"key_a": "val"},
        }
        h = ConfigurableHeuristic(
            name="test_and",
            source_node_type="function",
            metadata_keys=["key_a", "key_b"],
            terminal_actions=[TerminalAction.MARK_EVALUATED],
            metadata_key_op="AND",
        )
        store = _make_store([node_both, node_one])
        try:
            candidates = h.find(store)
            assert len(candidates) == 1
            assert candidates[0].source_urn == "urn:test:node:both"
        finally:
            store.stop_watcher()

    def test_empty_metadata_keys_matches_all(self):
        nodes = [
            {"urn": "urn:test:node:1", "node_type": "function", "metadata": {}},
            {"urn": "urn:test:node:2", "node_type": "function", "metadata": {"x": 1}},
        ]
        h = ConfigurableHeuristic(
            name="test_all",
            source_node_type="function",
            metadata_keys=[],
            terminal_actions=[TerminalAction.MARK_EVALUATED],
        )
        store = _make_store(nodes)
        try:
            assert len(h.find(store)) == 2
        finally:
            store.stop_watcher()

    def test_get_instructions_uses_provided_text(self):
        h = ConfigurableHeuristic(
            name="test",
            source_node_type="function",
            metadata_keys=[],
            terminal_actions=[TerminalAction.MARK_EVALUATED],
            instructions="Custom investigation text.",
        )
        assert h.get_instructions() == "Custom investigation text."

    def test_get_instructions_auto_generated(self):
        h = ConfigurableHeuristic(
            name="test",
            source_node_type="file",
            metadata_keys=["foo", "bar"],
            terminal_actions=[TerminalAction.MARK_EVALUATED],
            metadata_key_op="AND",
        )
        instr = h.get_instructions()
        assert "file" in instr
        assert "(AND)" in instr
        assert "foo" in instr
        assert "bar" in instr

    def test_serialization_roundtrip(self):
        h = ConfigurableHeuristic(
            name="roundtrip",
            source_node_type="dependency",
            metadata_keys=["cve_ids", "severity"],
            terminal_actions=[TerminalAction.MARK_EVALUATED, TerminalAction.CREATE_PR],
            metadata_key_op="AND",
            instructions="Check CVEs.",
            skill_content="## Playbook\nInvestigate CVEs.",
        )
        restored = ConfigurableHeuristic.from_dict(h.to_dict())
        assert restored.name == h.name
        assert restored.source_node_type == h.source_node_type
        assert restored.metadata_keys == h.metadata_keys
        assert restored.metadata_key_op == h.metadata_key_op
        assert restored.terminal_actions == h.terminal_actions
        assert restored.instructions == h.instructions
        assert restored.skill_content == h.skill_content

    def test_get_playbook_returns_skill_content(self):
        h = ConfigurableHeuristic(
            name="test",
            source_node_type="function",
            metadata_keys=[],
            terminal_actions=[TerminalAction.MARK_EVALUATED],
            skill_content="## My Playbook",
        )
        assert h.get_playbook() == "## My Playbook"

    def test_get_playbook_returns_none_when_empty(self):
        h = ConfigurableHeuristic(
            name="test",
            source_node_type="function",
            metadata_keys=[],
            terminal_actions=[TerminalAction.MARK_EVALUATED],
        )
        assert h.get_playbook() is None
