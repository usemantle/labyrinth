"""Tests for blast radius analysis (Feature 5)."""

import json
import os
import uuid

import pytest
from mcp.server.fastmcp import FastMCP

from src.mcp.graph_store import GraphStore
from src.mcp.tools.security import register

ORG_ID = str(uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"))


def _write_graph(tmp_path, nodes, edges):
    path = os.path.join(str(tmp_path), "graph.json")
    data = {
        "generated_at": "2024-01-01T00:00:00Z",
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
    }
    with open(path, "w") as f:
        json.dump(data, f)
    return path


def _make_blast_store(tmp_path):
    """Build a synthetic graph:
    func_a -> func_b (CODE_TO_CODE)
    func_b -> table_users (CODE_TO_DATA)
    table_users -> table_orders (DATA_TO_DATA via FK)
    table_users contains col_email (CONTAINS)
    func_a -> dep_requests (DEPENDS_ON)
    role_reader -> table_users (PRINCIPAL_TO_DATA)
    """
    nodes = [
        {"urn": "urn:test:::func_a", "organization_id": ORG_ID, "parent_urn": None,
         "node_type": "function",
         "metadata": {"function_name": "func_a", "file_path": "src/api.py"}},
        {"urn": "urn:test:::func_b", "organization_id": ORG_ID, "parent_urn": None,
         "node_type": "function",
         "metadata": {"function_name": "func_b", "file_path": "src/service.py"}},
        {"urn": "urn:test:::table_users", "organization_id": ORG_ID, "parent_urn": None,
         "node_type": "table",
         "metadata": {"table_name": "users", "data_sensitivity": "pii.email"}},
        {"urn": "urn:test:::col_email", "organization_id": ORG_ID,
         "parent_urn": "urn:test:::table_users",
         "node_type": "column",
         "metadata": {"column_name": "email", "data_sensitivity": "pii.email"}},
        {"urn": "urn:test:::table_orders", "organization_id": ORG_ID, "parent_urn": None,
         "node_type": "table",
         "metadata": {"table_name": "orders"}},
        {"urn": "urn:test:::dep_requests", "organization_id": ORG_ID, "parent_urn": None,
         "node_type": "dependency",
         "metadata": {"package_name": "requests", "package_version": "2.31.0",
                       "cve_ids": "CVE-2023-1234"}},
        {"urn": "urn:test:::role_reader", "organization_id": ORG_ID, "parent_urn": None,
         "node_type": "db_role",
         "metadata": {"role_name": "reader", "role_login": True, "role_superuser": False}},
        # An endpoint function for reverse blast radius
        {"urn": "urn:test:::endpoint_get", "organization_id": ORG_ID, "parent_urn": None,
         "node_type": "function",
         "metadata": {"function_name": "get_users", "file_path": "src/api.py",
                       "route_path": "/users", "http_method": "GET"}},
    ]
    edges = [
        {"uuid": str(uuid.uuid4()), "organization_id": ORG_ID,
         "from_urn": "urn:test:::func_a", "to_urn": "urn:test:::func_b",
         "relation_type": "CODE_TO_CODE", "metadata": {}},
        {"uuid": str(uuid.uuid4()), "organization_id": ORG_ID,
         "from_urn": "urn:test:::func_b", "to_urn": "urn:test:::table_users",
         "relation_type": "CODE_TO_DATA", "metadata": {}},
        {"uuid": str(uuid.uuid4()), "organization_id": ORG_ID,
         "from_urn": "urn:test:::table_users", "to_urn": "urn:test:::table_orders",
         "relation_type": "DATA_TO_DATA", "metadata": {}},
        {"uuid": str(uuid.uuid4()), "organization_id": ORG_ID,
         "from_urn": "urn:test:::table_users", "to_urn": "urn:test:::col_email",
         "relation_type": "CONTAINS", "metadata": {}},
        {"uuid": str(uuid.uuid4()), "organization_id": ORG_ID,
         "from_urn": "urn:test:::func_a", "to_urn": "urn:test:::dep_requests",
         "relation_type": "DEPENDS_ON", "metadata": {}},
        {"uuid": str(uuid.uuid4()), "organization_id": ORG_ID,
         "from_urn": "urn:test:::role_reader", "to_urn": "urn:test:::table_users",
         "relation_type": "PRINCIPAL_TO_DATA", "metadata": {"privilege": "SELECT"}},
        # endpoint_get -> func_b (CODE_TO_CODE)
        {"uuid": str(uuid.uuid4()), "organization_id": ORG_ID,
         "from_urn": "urn:test:::endpoint_get", "to_urn": "urn:test:::func_b",
         "relation_type": "CODE_TO_CODE", "metadata": {}},
        # endpoint_get -> table_users (CODE_TO_DATA)
        {"uuid": str(uuid.uuid4()), "organization_id": ORG_ID,
         "from_urn": "urn:test:::endpoint_get", "to_urn": "urn:test:::table_users",
         "relation_type": "CODE_TO_DATA", "metadata": {}},
    ]
    path = _write_graph(tmp_path, nodes, edges)
    return GraphStore(path)


@pytest.fixture
def blast_store(tmp_path):
    return _make_blast_store(tmp_path)


def _get_tool(store, tool_name):
    mcp = FastMCP("test")
    register(mcp, store)
    return mcp._tool_manager._tools[tool_name].fn


class TestBlastRadius:
    def test_blast_radius_from_function(self, blast_store):
        fn = _get_tool(blast_store, "blast_radius")
        result = fn(urn="urn:test:::func_a", max_depth=5)
        assert "func_b" in result
        assert "users" in result

    def test_blast_radius_follows_call_chain(self, blast_store):
        fn = _get_tool(blast_store, "blast_radius")
        result = fn(urn="urn:test:::func_a", max_depth=5)
        # func_a -> func_b -> table_users
        assert "users" in result
        assert "orders" in result

    def test_blast_radius_from_table(self, blast_store):
        fn = _get_tool(blast_store, "blast_radius")
        result = fn(urn="urn:test:::table_users", max_depth=5)
        assert "orders" in result
        assert "email" in result

    def test_blast_radius_depth_limit(self, blast_store):
        fn = _get_tool(blast_store, "blast_radius")
        result = fn(urn="urn:test:::func_a", max_depth=1)
        # At depth 1, should reach func_b and dep_requests but not table_users
        assert "func_b" in result
        assert "requests" in result

    def test_blast_radius_empty_result(self, tmp_path):
        nodes = [{"urn": "urn:test:::isolated", "organization_id": ORG_ID,
                   "parent_urn": None, "node_type": "function",
                   "metadata": {"function_name": "isolated", "file_path": "x.py"}}]
        path = _write_graph(tmp_path, nodes, [])
        store = GraphStore(path)
        fn = _get_tool(store, "blast_radius")
        result = fn(urn="urn:test:::isolated", max_depth=5)
        assert "Total reachable: 0" in result

    def test_blast_radius_includes_sensitivity_tags(self, blast_store):
        fn = _get_tool(blast_store, "blast_radius")
        result = fn(urn="urn:test:::func_a", max_depth=5)
        assert "SENSITIVE" in result
        assert "pii.email" in result

    def test_blast_radius_includes_cve_info(self, blast_store):
        fn = _get_tool(blast_store, "blast_radius")
        result = fn(urn="urn:test:::func_a", max_depth=5)
        assert "CVE-2023-1234" in result


class TestReverseBlastRadius:
    def test_reverse_blast_radius_from_table(self, blast_store):
        fn = _get_tool(blast_store, "reverse_blast_radius")
        result = fn(urn="urn:test:::table_users", max_depth=5)
        assert "func_b" in result

    def test_reverse_blast_radius_includes_principals(self, blast_store):
        fn = _get_tool(blast_store, "reverse_blast_radius")
        result = fn(urn="urn:test:::table_users", max_depth=5)
        assert "reader" in result
        assert "Principals with access" in result

    def test_reverse_blast_radius_shows_endpoints(self, blast_store):
        fn = _get_tool(blast_store, "reverse_blast_radius")
        result = fn(urn="urn:test:::table_users", max_depth=5)
        assert "Endpoints exposing this data" in result
        assert "get_users" in result
        assert "no auth detected" in result
