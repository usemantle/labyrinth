"""Tests for candidate filtering."""

from __future__ import annotations

import json
import tempfile
import uuid

from src.agent.candidates import Candidate, filter_already_linked
from src.mcp.graph_store import GraphStore


def _make_candidate(source_urn: str, target_edge_type: str = "builds") -> Candidate:
    return Candidate(
        source_urn=source_urn,
        source_node_type="file",
        source_metadata={},
        heuristic_name="test",
        target_edge_type=target_edge_type,
        target_node_type="image_repository",
        skill_file="",
    )


def _make_store(nodes: list[dict], edges: list[dict] | None = None) -> GraphStore:
    data = {
        "generated_at": "2024-01-01T00:00:00Z",
        "nodes": nodes,
        "edges": edges or [],
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(data, f)
        path = f.name
    return GraphStore(path)


class TestFilterAlreadyLinked:
    def test_keeps_unlinked_candidates(self):
        nodes = [
            {"urn": "urn:a", "node_type": "file", "metadata": {}},
            {"urn": "urn:b", "node_type": "image_repository", "metadata": {}},
        ]
        store = _make_store(nodes)
        try:
            candidates = [_make_candidate("urn:a")]
            result = filter_already_linked(candidates, store)
            assert len(result) == 1
        finally:
            store.stop_watcher()

    def test_removes_linked_candidates(self):
        nodes = [
            {"urn": "urn:a", "node_type": "file", "metadata": {}},
            {"urn": "urn:b", "node_type": "image_repository", "metadata": {}},
        ]
        edges = [
            {
                "uuid": str(uuid.uuid4()),
                "from_urn": "urn:a",
                "to_urn": "urn:b",
                "edge_type": "builds",
                "metadata": {},
            }
        ]
        store = _make_store(nodes, edges)
        try:
            candidates = [_make_candidate("urn:a", "builds")]
            result = filter_already_linked(candidates, store)
            assert len(result) == 0
        finally:
            store.stop_watcher()

    def test_different_edge_type_not_filtered(self):
        nodes = [
            {"urn": "urn:a", "node_type": "file", "metadata": {}},
            {"urn": "urn:b", "node_type": "image_repository", "metadata": {}},
        ]
        edges = [
            {
                "uuid": str(uuid.uuid4()),
                "from_urn": "urn:a",
                "to_urn": "urn:b",
                "edge_type": "contains",
                "metadata": {},
            }
        ]
        store = _make_store(nodes, edges)
        try:
            candidates = [_make_candidate("urn:a", "builds")]
            result = filter_already_linked(candidates, store)
            assert len(result) == 1
        finally:
            store.stop_watcher()

    def test_empty_candidates(self):
        store = _make_store([])
        try:
            assert filter_already_linked([], store) == []
        finally:
            store.stop_watcher()

    def test_incoming_edge_direction_filters_correctly(self):
        """ECR repos check incoming builds edges, not outgoing."""
        nodes = [
            {"urn": "urn:dockerfile", "node_type": "file", "metadata": {}},
            {"urn": "urn:ecr", "node_type": "image_repository", "metadata": {}},
        ]
        edges = [
            {
                "uuid": str(uuid.uuid4()),
                "from_urn": "urn:dockerfile",
                "to_urn": "urn:ecr",
                "edge_type": "builds",
                "metadata": {},
            }
        ]
        store = _make_store(nodes, edges)
        try:
            # Candidate with incoming direction should be filtered out
            candidate = Candidate(
                source_urn="urn:ecr",
                source_node_type="image_repository",
                source_metadata={},
                heuristic_name="orphaned_ecr_repo",
                target_edge_type="builds",
                target_node_type="file",
                skill_file="",
                edge_direction="incoming",
            )
            result = filter_already_linked([candidate], store)
            assert len(result) == 0
        finally:
            store.stop_watcher()

    def test_incoming_edge_direction_keeps_unlinked(self):
        """ECR repo with no incoming builds edge stays as candidate."""
        nodes = [
            {"urn": "urn:ecr", "node_type": "image_repository", "metadata": {}},
        ]
        store = _make_store(nodes)
        try:
            candidate = Candidate(
                source_urn="urn:ecr",
                source_node_type="image_repository",
                source_metadata={},
                heuristic_name="orphaned_ecr_repo",
                target_edge_type="builds",
                target_node_type="file",
                skill_file="",
                edge_direction="incoming",
            )
            result = filter_already_linked([candidate], store)
            assert len(result) == 1
        finally:
            store.stop_watcher()
