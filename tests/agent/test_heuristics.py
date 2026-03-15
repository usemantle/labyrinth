"""Tests for deterministic heuristics that query the graph."""

from __future__ import annotations

import json
import tempfile

import pytest

from src.agent.heuristics import (
    OrphanedEcrRepo,
    UnlinkedDockerfile,
    UnlinkedOrmModel,
    UnlinkedS3Code,
    gather_all_candidates,
)
from src.mcp.graph_store import GraphStore


def _make_store(nodes: list[dict], edges: list[dict] | None = None) -> GraphStore:
    """Create a GraphStore from synthetic graph data."""
    data = {
        "generated_at": "2024-01-01T00:00:00Z",
        "nodes": nodes,
        "edges": edges or [],
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


@pytest.fixture
def orm_class():
    return {
        "urn": "urn:github:repo:org:::org/app/src/models.py::User",
        "node_type": "class",
        "metadata": {
            "class_name": "User",
            "orm_table": "users",
            "orm_framework": "sqlalchemy",
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
            assert candidates[0].target_edge_type == "builds"
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
        instructions = UnlinkedDockerfile.get_instructions()
        assert "Dockerfile" in instructions
        assert "builds" in instructions


class TestUnlinkedS3Code:
    heuristic = UnlinkedS3Code()

    def test_returns_function_with_s3_ops(self, function_with_s3):
        store = _make_store([function_with_s3])
        try:
            candidates = self.heuristic.find(store)
            assert len(candidates) == 1
            assert candidates[0].heuristic_name == "unlinked_s3_code"
            assert candidates[0].target_edge_type == "writes"
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
        instructions = UnlinkedS3Code.get_instructions()
        assert "S3" in instructions
        assert "aws_s3_operations" in instructions


class TestUnlinkedOrmModel:
    heuristic = UnlinkedOrmModel()

    def test_returns_orm_class_without_models_edge(self, orm_class):
        store = _make_store([orm_class])
        try:
            candidates = self.heuristic.find(store)
            assert len(candidates) == 1
            assert candidates[0].heuristic_name == "unlinked_orm_model"
            assert candidates[0].target_edge_type == "models"
        finally:
            store.stop_watcher()

    def test_ignores_non_orm_classes(self):
        node = {
            "urn": "urn:github:repo:org:::org/app/src/utils.py::Helper",
            "node_type": "class",
            "metadata": {"class_name": "Helper"},
        }
        store = _make_store([node])
        try:
            assert self.heuristic.find(store) == []
        finally:
            store.stop_watcher()

    def test_get_instructions(self):
        instructions = UnlinkedOrmModel.get_instructions()
        assert "orm_table" in instructions
        assert "models" in instructions


class TestOrphanedEcrRepo:
    heuristic = OrphanedEcrRepo()

    def test_returns_ecr_repo(self, ecr_node):
        store = _make_store([ecr_node])
        try:
            candidates = self.heuristic.find(store)
            assert len(candidates) == 1
            assert candidates[0].heuristic_name == "orphaned_ecr_repo"
            assert candidates[0].target_node_type == "file"
        finally:
            store.stop_watcher()

    def test_no_ecr_repos(self):
        store = _make_store([])
        try:
            assert self.heuristic.find(store) == []
        finally:
            store.stop_watcher()

    def test_get_instructions(self):
        instructions = OrphanedEcrRepo.get_instructions()
        assert "ECR" in instructions
        assert "builds" in instructions


class TestGatherAll:
    def test_combines_all_heuristics(self, dockerfile_node, ecr_node, function_with_s3, orm_class):
        store = _make_store([dockerfile_node, ecr_node, function_with_s3, orm_class])
        try:
            candidates = gather_all_candidates(store)
            heuristic_names = {c.heuristic_name for c in candidates}
            assert "unlinked_dockerfile" in heuristic_names
            assert "unlinked_s3_code" in heuristic_names
            assert "unlinked_orm_model" in heuristic_names
            assert "orphaned_ecr_repo" in heuristic_names
        finally:
            store.stop_watcher()
