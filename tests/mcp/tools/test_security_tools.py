"""Tests for security MCP tools (Feature 1 tools: find_sensitive_data, trace_sensitive_data_access)."""

import json
import os
import tempfile
import uuid

import pytest

from src.mcp.graph_store import GraphStore


ORG_ID = str(uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"))


def _make_graph_json(nodes, edges):
    return json.dumps({
        "generated_at": "2024-01-01T00:00:00Z",
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
    })


def _write_graph(tmp_path, nodes, edges):
    path = os.path.join(str(tmp_path), "graph.json")
    with open(path, "w") as f:
        f.write(_make_graph_json(nodes, edges))
    return path


def _make_store_with_sensitive_data(tmp_path):
    """Create a store with a table that has sensitive columns and code accessing it."""
    table_urn = "urn:test:db:::mydb/public/users"
    col_urn = "urn:test:db:::mydb/public/users/email"
    func_urn = "urn:github:repo:::myapp/src/api.py/get_user"
    edge_uuid = str(uuid.uuid4())

    nodes = [
        {
            "urn": table_urn,
            "organization_id": ORG_ID,
            "parent_urn": None,
            "node_type": "table",
            "metadata": {"table_name": "users", "data_sensitivity": "pii.email"},
        },
        {
            "urn": col_urn,
            "organization_id": ORG_ID,
            "parent_urn": table_urn,
            "node_type": "column",
            "metadata": {"column_name": "email", "data_sensitivity": "pii.email"},
        },
        {
            "urn": func_urn,
            "organization_id": ORG_ID,
            "parent_urn": None,
            "node_type": "function",
            "metadata": {"function_name": "get_user", "file_path": "src/api.py"},
        },
    ]
    edges = [
        {
            "uuid": str(uuid.uuid4()),
            "organization_id": ORG_ID,
            "from_urn": table_urn,
            "to_urn": col_urn,
            "relation_type": "CONTAINS",
            "metadata": {},
        },
        {
            "uuid": edge_uuid,
            "organization_id": ORG_ID,
            "from_urn": func_urn,
            "to_urn": table_urn,
            "relation_type": "CODE_TO_DATA",
            "metadata": {"detection_method": "orm_reference"},
        },
    ]

    path = _write_graph(tmp_path, nodes, edges)
    return GraphStore(path)


@pytest.fixture
def sensitive_store(tmp_path):
    return _make_store_with_sensitive_data(tmp_path)


class TestFindSensitiveData:
    def test_returns_tagged_nodes(self, sensitive_store):
        from src.mcp.tools.security import register
        from mcp.server.fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp, sensitive_store)

        # Call the tool function directly
        result = mcp._tool_manager._tools["find_sensitive_data"].fn(category="")
        assert "email" in result
        assert "pii.email" in result

    def test_filters_by_category(self, sensitive_store):
        from src.mcp.tools.security import register
        from mcp.server.fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp, sensitive_store)

        result = mcp._tool_manager._tools["find_sensitive_data"].fn(category="secret")
        assert "No sensitive data found" in result

    def test_pii_category_matches(self, sensitive_store):
        from src.mcp.tools.security import register
        from mcp.server.fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp, sensitive_store)

        result = mcp._tool_manager._tools["find_sensitive_data"].fn(category="pii")
        assert "pii.email" in result


class TestTraceSensitiveDataAccess:
    def test_shows_code(self, sensitive_store):
        from src.mcp.tools.security import register
        from mcp.server.fastmcp import FastMCP

        mcp = FastMCP("test")
        register(mcp, sensitive_store)

        result = mcp._tool_manager._tools["trace_sensitive_data_access"].fn(table_name="users")
        assert "get_user" in result
        assert "pii.email" in result

    def test_no_sensitive_columns(self, tmp_path):
        from src.mcp.tools.security import register
        from mcp.server.fastmcp import FastMCP

        nodes = [{
            "urn": "urn:test:db:::mydb/public/logs",
            "organization_id": ORG_ID,
            "parent_urn": None,
            "node_type": "table",
            "metadata": {"table_name": "logs"},
        }]
        path = _write_graph(tmp_path, nodes, [])
        store = GraphStore(path)

        mcp = FastMCP("test")
        register(mcp, store)

        result = mcp._tool_manager._tools["trace_sensitive_data_access"].fn(table_name="logs")
        assert "No sensitive columns found" in result
