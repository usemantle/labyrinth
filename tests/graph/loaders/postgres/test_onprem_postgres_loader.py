"""
Unit tests for OnPremPostgresLoader.

All driver calls are mocked — no database required.
"""

import uuid
from unittest.mock import MagicMock, patch

import pytest

from labyrinth.drivers.sql.models import (
    ColumnMetadata,
    ForeignKeyMetadata,
    SchemaMetadata,
    TableMetadata,
)
from labyrinth.graph.loaders.postgres.onprem_postgres_loader import OnPremPostgresLoader

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
RESOURCE = "postgresql://user:pass@db.internal:5432/mydb"


def _build_mock_driver():
    driver = MagicMock()

    driver.discover_schemas.return_value = [
        SchemaMetadata(schema_name="public", database_name="mydb"),
    ]

    driver.discover_tables.return_value = [
        TableMetadata(table_name="users", table_type="BASE_TABLE"),
        TableMetadata(table_name="orders", table_type="BASE_TABLE"),
    ]

    # Column sets keyed by table
    def columns_for(schema_name, table_name):
        if table_name == "users":
            return [
                ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                ColumnMetadata(
                    column_name="email",
                    data_type="character varying",
                    is_nullable=False,
                    character_maximum_length=255,
                ),
            ]
        elif table_name == "orders":
            return [
                ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                ColumnMetadata(column_name="user_id", data_type="integer", is_nullable=False),
                ColumnMetadata(
                    column_name="total",
                    data_type="numeric",
                    is_nullable=True,
                    numeric_precision=10,
                    numeric_scale=2,
                ),
            ]
        return []

    driver.discover_columns.side_effect = columns_for

    driver.discover_foreign_keys.return_value = [
        ForeignKeyMetadata(
            constraint_name="orders_user_id_fkey",
            fk_schema="public",
            fk_table="orders",
            fk_column="user_id",
            ref_schema="public",
            ref_table="users",
            ref_column="id",
            ordinal_position=1,
        ),
    ]

    return driver


@pytest.fixture()
def loader_result():
    """Run the loader with a mocked driver, return (nodes, edges)."""
    with patch(
        "labyrinth.graph.loaders.postgres.postgres_loader.BaseDiscoveryDriver.get_driver"
    ) as mock_get:
        mock_get.return_value = _build_mock_driver()
        loader = OnPremPostgresLoader(organization_id=ORG_ID, resource=RESOURCE)
        return loader.load(RESOURCE)


# ── Node count & types ──────────────────────────────────────────────


def test_node_count(loader_result):
    nodes, _ = loader_result
    # 1 db + 1 schema + 2 tables + (2 + 3) columns = 9
    assert len(nodes) == 9


def test_database_node(loader_result):
    nodes, _ = loader_result
    db = nodes[0]
    assert db.parent_urn is None
    assert db.metadata["database_name"] == "mydb"
    assert db.metadata["host"] == "db.internal"
    assert db.metadata["port"] == 5432
    assert str(db.urn) == "urn:onprem:postgres:db.internal:5432:mydb"


def test_schema_node(loader_result):
    nodes, _ = loader_result
    schema = nodes[1]
    assert schema.metadata["schema_name"] == "public"
    assert str(schema.parent_urn) == str(nodes[0].urn)


def test_table_nodes(loader_result):
    nodes, _ = loader_result
    tables = [n for n in nodes if "table_name" in n.metadata]
    assert len(tables) == 2
    names = {n.metadata["table_name"] for n in tables}
    assert names == {"users", "orders"}
    for t in tables:
        assert t.metadata["table_type"] == "BASE_TABLE"


def test_column_nodes(loader_result):
    nodes, _ = loader_result
    columns = [n for n in nodes if "column_name" in n.metadata]
    assert len(columns) == 5
    names = {n.metadata["column_name"] for n in columns}
    assert names == {"id", "email", "user_id", "total"}


# ── Column metadata formatting ──────────────────────────────────────


def test_varchar_data_type(loader_result):
    nodes, _ = loader_result
    email = next(n for n in nodes if n.metadata.get("column_name") == "email")
    assert email.metadata["data_type"] == "character varying(255)"


def test_numeric_precision_scale(loader_result):
    nodes, _ = loader_result
    total = next(n for n in nodes if n.metadata.get("column_name") == "total")
    assert total.metadata["data_type"] == "numeric(10,2)"


def test_plain_data_type(loader_result):
    nodes, _ = loader_result
    # First "id" column (users.id)
    id_cols = [n for n in nodes if n.metadata.get("column_name") == "id"]
    assert id_cols[0].metadata["data_type"] == "integer"


def test_nullable_flag(loader_result):
    nodes, _ = loader_result
    total = next(n for n in nodes if n.metadata.get("column_name") == "total")
    assert total.metadata["nullable"] is True

    email = next(n for n in nodes if n.metadata.get("column_name") == "email")
    assert email.metadata["nullable"] is False


def test_ordinal_position(loader_result):
    nodes, _ = loader_result
    email = next(n for n in nodes if n.metadata.get("column_name") == "email")
    assert email.metadata["ordinal_position"] == 1


# ── Edge count & types ──────────────────────────────────────────────


def test_edge_counts(loader_result):
    _, edges = loader_result
    contains = [e for e in edges if e.edge_type == "contains"]
    fk = [e for e in edges if e.edge_type == "references"]
    # CONTAINS: 1 db→schema + 2 schema→table + 5 table→column = 8
    assert len(contains) == 8
    # DATA_TO_DATA: 1 FK
    assert len(fk) == 1


def test_contains_db_to_schema(loader_result):
    _, edges = loader_result
    db_to_schema = [
        e for e in edges
        if e.edge_type == "contains"
        and "mydb" == e.from_urn.path
    ]
    assert len(db_to_schema) == 1
    assert db_to_schema[0].to_urn.path == "mydb/public"


def test_fk_edge(loader_result):
    _, edges = loader_result
    fk = [e for e in edges if e.edge_type == "references"]
    assert len(fk) == 1
    edge = fk[0]
    assert edge.from_urn.path == "mydb/public/orders/user_id"
    assert edge.to_urn.path == "mydb/public/users/id"
    assert edge.metadata["constraint_name"] == "orders_user_id_fkey"
    assert edge.metadata["ordinal_position"] == 1


# ── URN hierarchy ───────────────────────────────────────────────────


def test_parent_urn_chain(loader_result):
    nodes, _ = loader_result
    email = next(n for n in nodes if n.metadata.get("column_name") == "email")
    users_table = next(n for n in nodes if n.metadata.get("table_name") == "users")
    schema = next(n for n in nodes if n.metadata.get("schema_name") == "public")
    db = nodes[0]

    assert email.parent_urn == users_table.urn
    assert users_table.parent_urn == schema.urn
    assert schema.parent_urn == db.urn
    assert db.parent_urn is None


def test_urn_parent_method_matches_parent_urn(loader_result):
    nodes, _ = loader_result
    email = next(n for n in nodes if n.metadata.get("column_name") == "email")
    assert email.urn.parent() == email.parent_urn


# ── Deterministic edge UUIDs ────────────────────────────────────────


def test_edge_uuids_deterministic(loader_result):
    """Running the loader twice produces identical edge UUIDs."""
    _, edges1 = loader_result

    with patch(
        "labyrinth.graph.loaders.postgres.postgres_loader.BaseDiscoveryDriver.get_driver"
    ) as mock_get:
        mock_get.return_value = _build_mock_driver()
        loader = OnPremPostgresLoader(organization_id=ORG_ID, resource=RESOURCE)
        _, edges2 = loader.load(RESOURCE)

    uuids1 = {e.uuid for e in edges1}
    uuids2 = {e.uuid for e in edges2}
    assert uuids1 == uuids2


# ── Organization isolation ──────────────────────────────────────────


def test_all_nodes_have_org_id(loader_result):
    nodes, _ = loader_result
    for node in nodes:
        assert node.organization_id == ORG_ID


def test_all_edges_have_org_id(loader_result):
    _, edges = loader_result
    for edge in edges:
        assert edge.organization_id == ORG_ID


# ── Empty database ──────────────────────────────────────────────────


def test_empty_database():
    """A database with no schemas produces only the db node."""
    driver = MagicMock()
    driver.discover_schemas.return_value = []
    driver.discover_foreign_keys.return_value = []

    with patch(
        "labyrinth.graph.loaders.postgres.postgres_loader.BaseDiscoveryDriver.get_driver"
    ) as mock_get:
        mock_get.return_value = driver
        loader = OnPremPostgresLoader(organization_id=ORG_ID, resource=RESOURCE)
        nodes, edges = loader.load(RESOURCE)

    assert len(nodes) == 1
    assert len(edges) == 0
    assert nodes[0].metadata["database_name"] == "mydb"
