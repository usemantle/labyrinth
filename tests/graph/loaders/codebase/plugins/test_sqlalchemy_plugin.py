"""
Unit tests for the SQLAlchemy codebase plugin.

Verifies detection of __tablename__ in Python ORM classes,
SQLAlchemy session operations in function bodies, and
CODE_TO_CODE edges from functions to ORM classes.
"""

import uuid

from labyrinth.graph.graph_models import (
    EdgeMetadataKey,
    NodeMetadataKey,
)
from labyrinth.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from labyrinth.graph.loaders.codebase.plugins import SQLAlchemyPlugin

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

NK = NodeMetadataKey


def test_sqlalchemy_plugin_detects_tablename(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "models.py").write_text(
        'from sqlalchemy import Column\n'
        'class User:\n'
        '    __tablename__ = "users"\n'
        '    id: int\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    user = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "User")
    assert user.metadata[NK.ORM_TABLE] == "users"
    assert user.metadata[NK.ORM_FRAMEWORK] == "sqlalchemy"


def test_sqlalchemy_plugin_ignores_non_orm_classes(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "services.py").write_text(
        'from sqlalchemy.orm import Session\n'
        'class UserService:\n'
        '    def get_user(self):\n'
        '        pass\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    svc = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "UserService")
    assert NK.ORM_TABLE not in svc.metadata


def test_sqlalchemy_plugin_ignores_javascript(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "model.js").write_text(
        'class User {\n'
        '    // __tablename__ = "users"  -- not Python!\n'
        '}\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    user = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "User")
    assert NK.ORM_TABLE not in user.metadata


def test_sqlalchemy_plugin_single_quotes(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "m.py").write_text(
        "from sqlalchemy import Column\n"
        "class Order:\n"
        "    __tablename__ = 'orders'\n"
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    order = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "Order")
    assert order.metadata[NK.ORM_TABLE] == "orders"


def test_sqlalchemy_plugin_multiple_models(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "models.py").write_text(
        'from sqlalchemy import Column\n'
        'class User:\n'
        '    __tablename__ = "users"\n'
        '\n'
        'class Order:\n'
        '    __tablename__ = "orders"\n'
        '\n'
        'class NotAModel:\n'
        '    pass\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    orm_nodes = [n for n in nodes if NK.ORM_TABLE in n.metadata]
    assert len(orm_nodes) == 2
    tables = {n.metadata[NK.ORM_TABLE] for n in orm_nodes}
    assert tables == {"users", "orders"}


def test_sqlalchemy_read_operations(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "crud.py").write_text(
        'from sqlalchemy.orm import Session\n'
        'def get_users(db):\n'
        '    return db.query(User).all()\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    func = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "get_users")
    assert func.metadata[NK.ORM_OPERATIONS] == "query"
    assert func.metadata[NK.ORM_OPERATION_TYPE] == "read"
    assert func.metadata[NK.ORM_FRAMEWORK] == "sqlalchemy"


def test_sqlalchemy_write_operations(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "crud.py").write_text(
        'from sqlalchemy.orm import Session\n'
        'def create_user(db, user):\n'
        '    db.add(user)\n'
        '    db.commit()\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    func = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "create_user")
    assert func.metadata[NK.ORM_OPERATIONS] == "add,commit"
    assert func.metadata[NK.ORM_OPERATION_TYPE] == "write"


def test_sqlalchemy_delete_operation(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "crud.py").write_text(
        'from sqlalchemy.orm import Session\n'
        'def remove_user(db, user):\n'
        '    db.delete(user)\n'
        '    db.commit()\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    func = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "remove_user")
    assert func.metadata[NK.ORM_OPERATIONS] == "commit,delete"
    assert func.metadata[NK.ORM_OPERATION_TYPE] == "delete,write"


def test_sqlalchemy_filter_delete(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "crud.py").write_text(
        'from sqlalchemy.orm import Session\n'
        'def purge_old(db):\n'
        '    db.query(User).filter(User.active == False).delete()\n'
        '    db.commit()\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    func = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "purge_old")
    assert "delete" in func.metadata[NK.ORM_OPERATIONS]
    assert "delete" in func.metadata[NK.ORM_OPERATION_TYPE]


def test_sqlalchemy_mixed_read_write(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "crud.py").write_text(
        'from sqlalchemy.orm import Session\n'
        'def upsert_user(db, data):\n'
        '    existing = db.query(User).get(data["id"])\n'
        '    if not existing:\n'
        '        db.add(User(**data))\n'
        '    db.commit()\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    func = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "upsert_user")
    assert func.metadata[NK.ORM_OPERATION_TYPE] == "read,write"


def test_sqlalchemy_no_orm_ops_no_tagging(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "utils.py").write_text(
        'def format_name(name):\n'
        '    return name.strip().title()\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    func = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "format_name")
    assert NK.ORM_OPERATIONS not in func.metadata
    assert NK.ORM_OPERATION_TYPE not in func.metadata


def test_sqlalchemy_operations_ignored_for_non_python(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "crud.js").write_text(
        'function getUsers(db) {\n'
        '    return db.query("SELECT * FROM users")\n'
        '}\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    func = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "getUsers")
    assert NK.ORM_OPERATIONS not in func.metadata


def test_sqlalchemy_ignores_decorator_methods(tmp_path):
    """@app.get() should not be detected as a SQLAlchemy .get() call."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "main.py").write_text(
        'from fastapi import FastAPI\n'
        '\n'
        'app = FastAPI()\n'
        '\n'
        '@app.get("/ping")\n'
        'def ping():\n'
        '    return {"message": "pong"}\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    ping = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "ping")
    assert NK.ORM_OPERATIONS not in ping.metadata
    assert NK.ORM_FRAMEWORK not in ping.metadata


def test_sqlalchemy_no_enrichment_without_import(tmp_path):
    """Files without sqlalchemy imports should not get ORM metadata."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "app.py").write_text(
        'def get_data(db):\n'
        '    return db.query("SELECT 1")\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, _ = loader.load(str(repo))
    func = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "get_data")
    assert NK.ORM_OPERATIONS not in func.metadata
    assert NK.ORM_FRAMEWORK not in func.metadata


# ── CODE_TO_CODE edge tests ──────────────────────────────────────────

EK = EdgeMetadataKey


def test_sqlalchemy_function_links_to_orm_class(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "models.py").write_text(
        'from sqlalchemy import Column\n'
        'class User:\n'
        '    __tablename__ = "users"\n'
        '    id: int\n'
    )
    (repo / "crud.py").write_text(
        'from sqlalchemy.orm import Session\n'
        'def get_users(db):\n'
        '    return db.query(User).all()\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, edges = loader.load(str(repo))
    func = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "get_users")
    user_cls = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "User")
    assert func.metadata[NK.ORM_MODELS] == "User"
    c2c = [e for e in edges if e.edge_type in ("calls", "instantiates")
           and e.from_urn == func.urn and e.to_urn == user_cls.urn]
    assert len(c2c) == 1
    assert c2c[0].metadata[EK.DETECTION_METHOD] == "orm_model_reference"
    assert c2c[0].metadata[EK.CONFIDENCE] == 0.9
    assert c2c[0].metadata[EK.ORM_FRAMEWORK] == "sqlalchemy"
    assert c2c[0].metadata[EK.ORM_CLASS] == "User"


def test_sqlalchemy_function_links_to_multiple_models(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "models.py").write_text(
        'from sqlalchemy import Column\n'
        'class User:\n'
        '    __tablename__ = "users"\n'
        '\n'
        'class Order:\n'
        '    __tablename__ = "orders"\n'
    )
    (repo / "crud.py").write_text(
        'from sqlalchemy.orm import Session\n'
        'def get_user_orders(db, user_id):\n'
        '    user = db.query(User).get(user_id)\n'
        '    orders = db.query(Order).filter(Order.user_id == user_id).all()\n'
        '    return user, orders\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, edges = loader.load(str(repo))
    func = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "get_user_orders")
    assert func.metadata[NK.ORM_MODELS] == "Order,User"
    c2c = [e for e in edges if e.edge_type in ("calls", "instantiates")
           and e.from_urn == func.urn]
    assert len(c2c) == 2
    linked_classes = {e.metadata[EK.ORM_CLASS] for e in c2c}
    assert linked_classes == {"User", "Order"}


def test_sqlalchemy_no_edge_when_no_class_reference(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "models.py").write_text(
        'from sqlalchemy import Column\n'
        'class User:\n'
        '    __tablename__ = "users"\n'
    )
    (repo / "crud.py").write_text(
        'from sqlalchemy.orm import Session\n'
        'def do_commit(db):\n'
        '    db.commit()\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, edges = loader.load(str(repo))
    func = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "do_commit")
    assert NK.ORM_MODELS not in func.metadata
    c2c = [e for e in edges if e.edge_type in ("calls", "instantiates")
           and e.from_urn == func.urn]
    assert len(c2c) == 0


def test_sqlalchemy_no_scan_without_orm_operations(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "models.py").write_text(
        'from sqlalchemy import Column\n'
        'class User:\n'
        '    __tablename__ = "users"\n'
    )
    (repo / "utils.py").write_text(
        'def make_user_dict(User):\n'
        '    return {"name": User.name}\n'
    )
    loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID, plugins=[SQLAlchemyPlugin()],
    )
    nodes, edges = loader.load(str(repo))
    func = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "make_user_dict")
    assert NK.ORM_MODELS not in func.metadata
    assert NK.ORM_OPERATIONS not in func.metadata
    c2c = [e for e in edges if e.edge_type in ("calls", "instantiates")
           and e.from_urn == func.urn]
    assert len(c2c) == 0
