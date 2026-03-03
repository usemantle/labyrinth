"""
Unit tests for the SQLAlchemy codebase plugin.

Verifies detection of __tablename__ in Python ORM classes.
"""

import uuid

from src.graph.graph_models import (
    NodeMetadataKey,
)
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from src.graph.loaders.codebase.plugins import SQLAlchemyPlugin

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

NK = NodeMetadataKey


def test_sqlalchemy_plugin_detects_tablename(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "models.py").write_text(
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
    """SQLAlchemy plugin only fires for Python."""
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
    """Handles __tablename__ = 'value' with single quotes."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "m.py").write_text(
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
    """Multiple ORM models in one file are all detected."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "models.py").write_text(
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
