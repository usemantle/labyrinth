"""Tests for PostgreSQL role and grant discovery (Feature 4)."""

import json
import os
import uuid
from unittest.mock import MagicMock, patch

from mcp.server.fastmcp import FastMCP

from src.drivers.sql.models import (
    ColumnMetadata,
    GrantMetadata,
    RoleMetadata,
    SchemaMetadata,
    TableMetadata,
)
from src.graph.graph_models import NodeMetadataKey, RelationType
from src.graph.loaders.postgres.onprem_postgres_loader import OnPremPostgresLoader
from src.graph.sinks.json_file_sink import classify_node
from src.mcp.graph_store import GraphStore
from src.mcp.tools.security import register

NK = NodeMetadataKey
ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
RESOURCE = "postgresql://user:pass@db.internal:5432/mydb"


def _build_mock_driver(roles=None, grants=None):
    driver = MagicMock()
    driver.discover_schemas.return_value = [
        SchemaMetadata(schema_name="public", database_name="mydb"),
    ]
    driver.discover_tables.return_value = [
        TableMetadata(table_name="users", table_type="BASE_TABLE"),
    ]
    driver.discover_columns.return_value = [
        ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
    ]
    driver.discover_foreign_keys.return_value = []
    driver.discover_roles.return_value = roles or []
    driver.discover_grants.return_value = grants or []
    return driver


def _load_with_mock(roles=None, grants=None):
    with patch(
        "src.graph.loaders.postgres.postgres_loader.BaseDiscoveryDriver.get_driver"
    ) as mock_get:
        mock_get.return_value = _build_mock_driver(roles, grants)
        loader = OnPremPostgresLoader(organization_id=ORG_ID, resource=RESOURCE)
        return loader.load(RESOURCE)


def _get_tool(store, tool_name):
    mcp = FastMCP("test")
    register(mcp, store)
    return mcp._tool_manager._tools[tool_name].fn


def _write_graph(tmp_path, nodes, edges):
    path = os.path.join(str(tmp_path), "graph.json")
    with open(path, "w") as f:
        json.dump({
            "generated_at": "2024-01-01", "node_count": len(nodes),
            "edge_count": len(edges), "nodes": nodes, "edges": edges,
        }, f)
    return path


# ── Role node tests ──────────────────────────────────────────────────


class TestRoleNodes:
    def test_role_nodes_created(self):
        roles = [
            RoleMetadata(role_name="app_user", can_login=True, is_superuser=False),
            RoleMetadata(role_name="admin", can_login=True, is_superuser=True),
        ]
        nodes, _ = _load_with_mock(roles=roles)
        role_nodes = [n for n in nodes if NK.ROLE_NAME in n.metadata]
        assert len(role_nodes) == 2
        names = {n.metadata[NK.ROLE_NAME] for n in role_nodes}
        assert names == {"app_user", "admin"}

    def test_role_metadata_login_superuser(self):
        roles = [
            RoleMetadata(role_name="admin", can_login=True, is_superuser=True),
        ]
        nodes, _ = _load_with_mock(roles=roles)
        admin = next(n for n in nodes if n.metadata.get(NK.ROLE_NAME) == "admin")
        assert admin.metadata[NK.ROLE_LOGIN] is True
        assert admin.metadata[NK.ROLE_SUPERUSER] is True

    def test_superuser_flagged(self):
        roles = [
            RoleMetadata(role_name="regular", can_login=True, is_superuser=False),
        ]
        nodes, _ = _load_with_mock(roles=roles)
        regular = next(n for n in nodes if n.metadata.get(NK.ROLE_NAME) == "regular")
        assert regular.metadata[NK.ROLE_SUPERUSER] is False

    def test_role_node_classification(self):
        roles = [
            RoleMetadata(role_name="app_user", can_login=True, is_superuser=False),
        ]
        nodes, _ = _load_with_mock(roles=roles)
        role_node = next(n for n in nodes if n.metadata.get(NK.ROLE_NAME) == "app_user")
        assert classify_node(role_node) == "db_role"


# ── Grant edge tests ─────────────────────────────────────────────────


class TestGrantEdges:
    def test_principal_to_data_edges_from_grants(self):
        roles = [RoleMetadata(role_name="app_user", can_login=True, is_superuser=False)]
        grants = [
            GrantMetadata(
                grantee="app_user",
                table_schema="public",
                table_name="users",
                privilege_type="SELECT",
                is_grantable=False,
            ),
        ]
        _, edges = _load_with_mock(roles=roles, grants=grants)
        p2d = [e for e in edges if e.relation_type == RelationType.PRINCIPAL_TO_DATA]
        assert len(p2d) == 1
        assert p2d[0].metadata["privilege"] == "SELECT"

    def test_multiple_privileges_multiple_edges(self):
        roles = [RoleMetadata(role_name="app_user", can_login=True, is_superuser=False)]
        grants = [
            GrantMetadata(grantee="app_user", table_schema="public",
                          table_name="users", privilege_type="SELECT", is_grantable=False),
            GrantMetadata(grantee="app_user", table_schema="public",
                          table_name="users", privilege_type="INSERT", is_grantable=False),
        ]
        _, edges = _load_with_mock(roles=roles, grants=grants)
        p2d = [e for e in edges if e.relation_type == RelationType.PRINCIPAL_TO_DATA]
        assert len(p2d) == 2

    def test_no_grants_no_edges(self):
        roles = [RoleMetadata(role_name="app_user", can_login=True, is_superuser=False)]
        _, edges = _load_with_mock(roles=roles, grants=[])
        p2d = [e for e in edges if e.relation_type == RelationType.PRINCIPAL_TO_DATA]
        assert len(p2d) == 0

    def test_grant_for_unknown_role_ignored(self):
        """Grants for roles not in the discovered roles list are skipped."""
        roles = [RoleMetadata(role_name="app_user", can_login=True, is_superuser=False)]
        grants = [
            GrantMetadata(grantee="unknown_role", table_schema="public",
                          table_name="users", privilege_type="SELECT", is_grantable=False),
        ]
        _, edges = _load_with_mock(roles=roles, grants=grants)
        p2d = [e for e in edges if e.relation_type == RelationType.PRINCIPAL_TO_DATA]
        assert len(p2d) == 0


# ── Integration test ──────────────────────────────────────────────────


class TestIntegration:
    def test_roles_plus_tables(self):
        roles = [RoleMetadata(role_name="reader", can_login=True, is_superuser=False)]
        grants = [
            GrantMetadata(grantee="reader", table_schema="public",
                          table_name="users", privilege_type="SELECT", is_grantable=False),
        ]
        nodes, edges = _load_with_mock(roles=roles, grants=grants)

        # Should have db + schema + table + column + role = 5 nodes
        assert len(nodes) == 5

        table_nodes = [n for n in nodes if NK.TABLE_NAME in n.metadata]
        role_nodes = [n for n in nodes if NK.ROLE_NAME in n.metadata]
        assert len(table_nodes) == 1
        assert len(role_nodes) == 1

        # Should have PRINCIPAL_TO_DATA edge
        p2d = [e for e in edges if e.relation_type == RelationType.PRINCIPAL_TO_DATA]
        assert len(p2d) == 1
        assert str(p2d[0].from_urn) == str(role_nodes[0].urn)
        assert str(p2d[0].to_urn) == str(table_nodes[0].urn)


# ── MCP tool test ─────────────────────────────────────────────────────


class TestMCPDatabasePermissions:
    def test_find_database_permissions_by_table(self, tmp_path):
        org = str(ORG_ID)
        role_urn = "urn:onprem:postgres:db:5432:mydb/roles/reader"
        table_urn = "urn:onprem:postgres:db:5432:mydb/public/users"

        nodes = [
            {"urn": role_urn, "organization_id": org, "parent_urn": None,
             "node_type": "db_role",
             "metadata": {"role_name": "reader", "role_login": True, "role_superuser": False}},
            {"urn": table_urn, "organization_id": org, "parent_urn": None,
             "node_type": "table",
             "metadata": {"table_name": "users"}},
        ]
        edges = [{
            "uuid": str(uuid.uuid4()),
            "organization_id": org,
            "from_urn": role_urn,
            "to_urn": table_urn,
            "relation_type": "PRINCIPAL_TO_DATA",
            "metadata": {"privilege": "SELECT"},
        }]

        path = _write_graph(tmp_path, nodes, edges)
        store = GraphStore(path)
        fn = _get_tool(store, "find_database_permissions")
        result = fn(table_name="users", role_name="")
        assert "reader" in result
        assert "SELECT" in result

    def test_find_database_permissions_by_role(self, tmp_path):
        org = str(ORG_ID)
        role_urn = "urn:onprem:postgres:db:5432:mydb/roles/admin"
        table_urn = "urn:onprem:postgres:db:5432:mydb/public/users"

        nodes = [
            {"urn": role_urn, "organization_id": org, "parent_urn": None,
             "node_type": "db_role",
             "metadata": {"role_name": "admin", "role_login": True, "role_superuser": True}},
            {"urn": table_urn, "organization_id": org, "parent_urn": None,
             "node_type": "table",
             "metadata": {"table_name": "users"}},
        ]
        edges = [{
            "uuid": str(uuid.uuid4()),
            "organization_id": org,
            "from_urn": role_urn,
            "to_urn": table_urn,
            "relation_type": "PRINCIPAL_TO_DATA",
            "metadata": {"privilege": "ALL"},
        }]

        path = _write_graph(tmp_path, nodes, edges)
        store = GraphStore(path)
        fn = _get_tool(store, "find_database_permissions")
        result = fn(table_name="", role_name="admin")
        assert "admin" in result
        assert "SUPERUSER" in result
