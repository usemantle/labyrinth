"""Tests for JsonFileSink persistence methods."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

from labyrinth.graph.graph_models import URN, Edge, Node, NodeMetadata
from labyrinth.graph.sinks.json_file_sink import JsonFileSink


@pytest.fixture
def sink(tmp_path: Path) -> JsonFileSink:
    return JsonFileSink(tmp_path / "graph.json")


@pytest.fixture
def org_id() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def sample_nodes(org_id: uuid.UUID) -> list[Node]:
    return [
        Node(
            organization_id=org_id,
            urn=URN("urn:test:svc:acct:region:path/a"),
            metadata=NodeMetadata({"file_path": "a.py"}),
            node_type="file",
        ),
        Node(
            organization_id=org_id,
            urn=URN("urn:test:svc:acct:region:path/b"),
            metadata=NodeMetadata({"function_name": "foo"}),
            node_type="function",
        ),
    ]


@pytest.fixture
def sample_edges(org_id: uuid.UUID) -> list[Edge]:
    return [
        Edge(
            uuid=uuid.uuid4(),
            organization_id=org_id,
            from_urn=URN("urn:test:svc:acct:region:path/a"),
            to_urn=URN("urn:test:svc:acct:region:path/b"),
            edge_type="contains",
        ),
    ]


class TestWrite:
    def test_creates_valid_structure(self, sink, sample_nodes, sample_edges):
        sink.write(sample_nodes, sample_edges)
        data = json.loads(sink._output_path.read_text())
        assert "nodes" in data
        assert "edges" in data
        assert "soft_links" in data
        assert len(data["nodes"]) == 2
        assert len(data["edges"]) == 1
        assert data["soft_links"] == []


class TestUpdateNodeMetadata:
    def test_merges_metadata(self, sink, sample_nodes, sample_edges):
        sink.write(sample_nodes, sample_edges)
        urn = "urn:test:svc:acct:region:path/a"
        sink.update_node_metadata(urn, evaluated_at="2024-01-01")
        data = json.loads(sink._output_path.read_text())
        node = next(n for n in data["nodes"] if n["urn"] == urn)
        assert node["metadata"]["evaluated_at"] == "2024-01-01"
        # Original metadata preserved
        assert node["metadata"]["file_path"] == "a.py"


class TestDeleteNodeMetadata:
    def test_removes_keys(self, sink, sample_nodes, sample_edges):
        sink.write(sample_nodes, sample_edges)
        urn = "urn:test:svc:acct:region:path/a"
        sink.update_node_metadata(urn, to_remove="yes", to_keep="yes")
        sink.delete_node_metadata(urn, "to_remove")
        data = json.loads(sink._output_path.read_text())
        node = next(n for n in data["nodes"] if n["urn"] == urn)
        assert "to_remove" not in node["metadata"]
        assert node["metadata"]["to_keep"] == "yes"


class TestAddSoftLink:
    def test_appends_soft_link(self, sink, sample_nodes, sample_edges):
        sink.write(sample_nodes, sample_edges)
        link = {"id": "link-1", "from_urn": "a", "to_urn": "b", "edge_type": "reads"}
        sink.add_soft_link(link)
        data = json.loads(sink._output_path.read_text())
        assert len(data["soft_links"]) == 1
        assert data["soft_links"][0]["id"] == "link-1"


class TestRemoveSoftLink:
    def test_removes_by_id(self, sink, sample_nodes, sample_edges):
        sink.write(sample_nodes, sample_edges)
        sink.add_soft_link({"id": "link-1", "from_urn": "a", "to_urn": "b"})
        sink.add_soft_link({"id": "link-2", "from_urn": "c", "to_urn": "d"})
        sink.remove_soft_link("link-1")
        data = json.loads(sink._output_path.read_text())
        assert len(data["soft_links"]) == 1
        assert data["soft_links"][0]["id"] == "link-2"


class TestAtomicWritePreservation:
    def test_preserves_other_sections(self, sink, sample_nodes, sample_edges):
        sink.write(sample_nodes, sample_edges)
        sink.add_soft_link({"id": "sl-1", "from_urn": "a", "to_urn": "b"})
        sink.update_node_metadata("urn:test:svc:acct:region:path/a", new_key="val")
        data = json.loads(sink._output_path.read_text())
        # All sections still present and correct
        assert len(data["nodes"]) == 2
        assert len(data["edges"]) == 1
        assert len(data["soft_links"]) == 1
        node = next(n for n in data["nodes"] if n["urn"] == "urn:test:svc:acct:region:path/a")
        assert node["metadata"]["new_key"] == "val"
