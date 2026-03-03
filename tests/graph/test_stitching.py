"""
Unit tests for the code-to-data stitching orchestrator.

Verifies that ORM classes and functions get linked to database tables
via CODE_TO_DATA edges.
"""

import uuid

from src.graph.graph_models import (
    EdgeMetadataKey,
    Node,
    NodeMetadata,
    NodeMetadataKey,
    RelationType,
    URN,
)
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from src.graph.loaders.codebase.plugins import SQLAlchemyPlugin
from src.graph.stitching import stitch_code_to_data

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

NK = NodeMetadataKey
EK = EdgeMetadataKey


# ── Helpers ────────────────────────────────────────────────────────────


def _make_orm_repo(tmp_path):
    """Create a repo with ORM models + an API file that uses them."""
    repo = tmp_path / "code"
    repo.mkdir()
    (repo / "models.py").write_text(
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
    """Create fake database nodes mimicking OnPremPostgresLoader output."""
    from src.graph.loaders._helpers import make_edge

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
        make_edge(ORG_ID, db_urn, schema_urn, RelationType.CONTAINS),
        make_edge(ORG_ID, schema_urn, users_urn, RelationType.CONTAINS),
        make_edge(ORG_ID, schema_urn, orders_urn, RelationType.CONTAINS),
    ]
    return nodes, edges


# ── Stitch tests ───────────────────────────────────────────────────────


def test_stitch_orm_class_to_table(tmp_path):
    """ORM classes with orm_table link to matching table nodes."""
    repo = _make_orm_repo(tmp_path)
    data_nodes, data_edges = _make_data_nodes()

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    code_nodes, code_edges = loader.load(str(repo))

    all_nodes, all_edges = stitch_code_to_data(
        ORG_ID, data_nodes, data_edges, code_nodes, code_edges,
        code_base_paths=[str(repo)],
    )

    c2d = [e for e in all_edges if e.relation_type == RelationType.CODE_TO_DATA]
    orm_edges = [e for e in c2d if e.metadata.get(EK.DETECTION_METHOD) == "orm_tablename"]

    # User → users, Order → orders
    assert len(orm_edges) == 2
    targets = {e.metadata[EK.TABLE_NAME] for e in orm_edges}
    assert targets == {"users", "orders"}
    assert all(e.metadata[EK.CONFIDENCE] == 1.0 for e in orm_edges)


def test_stitch_function_to_table(tmp_path):
    """Functions referencing ORM class names get CODE_TO_DATA edges."""
    repo = _make_orm_repo(tmp_path)
    data_nodes, data_edges = _make_data_nodes()

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    code_nodes, code_edges = loader.load(str(repo))

    all_nodes, all_edges = stitch_code_to_data(
        ORG_ID, data_nodes, data_edges, code_nodes, code_edges,
        code_base_paths=[str(repo)],
    )

    c2d = [e for e in all_edges if e.relation_type == RelationType.CODE_TO_DATA]
    func_edges = [e for e in c2d if e.metadata.get(EK.DETECTION_METHOD) == "orm_reference"]

    # create_user references User → links to users table
    # list_orders references Order → links to orders table
    # health_check references neither → no edge
    func_to_table = {
        str(e.from_urn).rsplit("/", 1)[-1]: e.metadata[EK.TABLE_NAME]
        for e in func_edges
    }
    assert "create_user" in func_to_table
    assert func_to_table["create_user"] == "users"
    assert "list_orders" in func_to_table
    assert func_to_table["list_orders"] == "orders"
    assert "health_check" not in func_to_table


def test_stitch_no_data_nodes(tmp_path):
    """No crash when data graph has no table nodes."""
    repo = _make_orm_repo(tmp_path)
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    code_nodes, code_edges = loader.load(str(repo))

    all_nodes, all_edges = stitch_code_to_data(
        ORG_ID, [], [], code_nodes, code_edges,
        code_base_paths=[str(repo)],
    )

    c2d = [e for e in all_edges if e.relation_type == RelationType.CODE_TO_DATA]
    assert len(c2d) == 0


def test_stitch_no_orm_models(tmp_path):
    """No crash when code graph has no ORM models."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "plain.py").write_text('def hello():\n    pass\n')

    data_nodes, data_edges = _make_data_nodes()
    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    code_nodes, code_edges = loader.load(str(repo))

    all_nodes, all_edges = stitch_code_to_data(
        ORG_ID, data_nodes, data_edges, code_nodes, code_edges,
        code_base_paths=[str(repo)],
    )

    c2d = [e for e in all_edges if e.relation_type == RelationType.CODE_TO_DATA]
    assert len(c2d) == 0


def test_stitch_edge_metadata(tmp_path):
    """CODE_TO_DATA edges have proper metadata."""
    repo = _make_orm_repo(tmp_path)
    data_nodes, data_edges = _make_data_nodes()

    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    code_nodes, code_edges = loader.load(str(repo))

    _, all_edges = stitch_code_to_data(
        ORG_ID, data_nodes, data_edges, code_nodes, code_edges,
        code_base_paths=[str(repo)],
    )

    c2d = [e for e in all_edges if e.relation_type == RelationType.CODE_TO_DATA]
    for edge in c2d:
        assert EK.DETECTION_METHOD in edge.metadata
        assert EK.CONFIDENCE in edge.metadata
        assert EK.TABLE_NAME in edge.metadata
        assert edge.organization_id == ORG_ID
