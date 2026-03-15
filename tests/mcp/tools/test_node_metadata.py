"""Tests for the update_node_metadata MCP tool."""

from __future__ import annotations

import json
import tempfile

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


class TestUpdateNodeMetadata:
    def test_update_valid_urn(self):
        nodes = [{"urn": "urn:a", "node_type": "file", "metadata": {"file_path": "a.py"}}]
        store = _make_store(nodes)
        try:
            store.update_node_metadata("urn:a", evaluated_at="2024-01-01")
            # In-memory check
            meta = store.G.nodes["urn:a"]["metadata"]
            assert meta["evaluated_at"] == "2024-01-01"
            assert meta["file_path"] == "a.py"
            # On-disk check
            with open(store._json_path) as f:
                data = json.load(f)
            node = data["nodes"][0]
            assert node["metadata"]["evaluated_at"] == "2024-01-01"
        finally:
            store.stop_watcher()

    def test_update_invalid_urn(self):
        import pytest

        store = _make_store([{"urn": "urn:a", "node_type": "file", "metadata": {}}])
        try:
            with pytest.raises(KeyError):
                store.update_node_metadata("urn:nonexistent", foo="bar")
        finally:
            store.stop_watcher()


class TestDeleteNodeMetadata:
    def test_delete_keys(self):
        nodes = [{"urn": "urn:a", "node_type": "file", "metadata": {"keep": "yes", "remove": "yes"}}]
        store = _make_store(nodes)
        try:
            store.delete_node_metadata("urn:a", "remove")
            meta = store.G.nodes["urn:a"]["metadata"]
            assert "remove" not in meta
            assert meta["keep"] == "yes"
        finally:
            store.stop_watcher()
