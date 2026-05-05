"""Tests for FunctionToTableStitcher."""

import uuid

from labyrinth.graph.graph_models import (
    URN,
    EdgeMetadataKey,
    Graph,
    Node,
    NodeMetadata,
    NodeMetadataKey,
)
from labyrinth.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from labyrinth.graph.loaders.codebase.plugins import SQLAlchemyPlugin
from labyrinth.graph.stitchers.function_to_table import FunctionToTableStitcher

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey
EK = EdgeMetadataKey


def _make_orm_repo(tmp_path):
    repo = tmp_path / "code"
    repo.mkdir()
    (repo / "models.py").write_text(
        'from sqlalchemy import Column\n'
        'class User:\n'
        '    __tablename__ = "users"\n'
        '    id: int\n'
        '    email: str\n'
        '\n'
        'class Order:\n'
        '    __tablename__ = "orders"\n'
        '    id: int\n'
    )
    (repo / "api.py").write_text(
        'from sqlalchemy.orm import Session\n'
        'from models import User\n'
        '\n'
        'def create_user():\n'
        '    user = User(email="test@example.com")\n'
        '    return user\n'
        '\n'
        'def list_orders():\n'
        '    orders = Order.query.all()\n'
        '    return orders\n'
        '\n'
        'def health_check():\n'
        '    return "ok"\n'
    )
    return repo


def _make_data_nodes():
    from labyrinth.graph.edges.contains_edge import ContainsEdge

    db_urn = URN("urn:onprem:postgres:localhost:5432:mydb")
    schema_urn = URN("urn:onprem:postgres:localhost:5432:mydb/public")
    users_urn = URN("urn:onprem:postgres:localhost:5432:mydb/public/users")
    orders_urn = URN("urn:onprem:postgres:localhost:5432:mydb/public/orders")

    nodes = [
        Node(organization_id=ORG_ID, urn=db_urn, metadata=NodeMetadata({NK.DATABASE_NAME: "mydb"})),
        Node(organization_id=ORG_ID, urn=schema_urn, parent_urn=db_urn, metadata=NodeMetadata({NK.SCHEMA_NAME: "public"})),
        Node(organization_id=ORG_ID, urn=users_urn, parent_urn=schema_urn, metadata=NodeMetadata({NK.TABLE_NAME: "users"})),
        Node(organization_id=ORG_ID, urn=orders_urn, parent_urn=schema_urn, metadata=NodeMetadata({NK.TABLE_NAME: "orders"})),
    ]
    edges = [
        ContainsEdge.create(ORG_ID, db_urn, schema_urn),
        ContainsEdge.create(ORG_ID, schema_urn, users_urn),
        ContainsEdge.create(ORG_ID, schema_urn, orders_urn),
    ]
    return nodes, edges


def test_stitch_function_to_table(tmp_path):
    """Functions referencing ORM class names get reads edges."""
    repo = _make_orm_repo(tmp_path)
    data_nodes, data_edges = _make_data_nodes()

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    code_nodes, code_edges = loader.load(str(repo))

    graph = Graph(nodes=data_nodes + code_nodes, edges=data_edges + code_edges)
    context = {"code_base_paths": [str(repo)]}
    result = FunctionToTableStitcher().stitch(ORG_ID, graph, context)

    func_edges = [e for e in result.edges if e.metadata.get(EK.DETECTION_METHOD) == "orm_reference"]

    func_to_table = {
        str(e.from_urn).rsplit("/", 1)[-1]: e.metadata[EK.TABLE_NAME]
        for e in func_edges
    }
    assert "create_user" in func_to_table
    assert func_to_table["create_user"] == "users"
    assert "list_orders" in func_to_table
    assert func_to_table["list_orders"] == "orders"
    assert "health_check" not in func_to_table
