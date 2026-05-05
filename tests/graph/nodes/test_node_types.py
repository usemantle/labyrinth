"""Tests for typed node subclasses."""

import uuid

import pytest

from labyrinth.graph.graph_models import URN, Node, NodeMetadataKey
from labyrinth.graph.nodes import (
    BucketNode,
    ClassNode,
    CodebaseNode,
    ColumnNode,
    DatabaseNode,
    DependencyNode,
    FileNode,
    FunctionNode,
    IdentityNode,
    ObjectPathNode,
    SchemaNode,
    TableNode,
)

NK = NodeMetadataKey
ORG_ID = uuid.uuid4()


class TestNodeSubclassIsNode:
    """Every typed node must be an instance of Node."""

    @pytest.mark.parametrize("node_cls", [
        CodebaseNode, FileNode, ClassNode, FunctionNode,
        DependencyNode, IdentityNode, DatabaseNode, SchemaNode,
        TableNode, ColumnNode, BucketNode, ObjectPathNode,
    ])
    def test_is_subclass_of_node(self, node_cls):
        assert issubclass(node_cls, Node)


class TestNodeType:
    """Each typed node must report the correct node_type."""

    @pytest.mark.parametrize("node_cls,expected_type", [
        (CodebaseNode, "codebase"),
        (FileNode, "file"),
        (ClassNode, "class"),
        (FunctionNode, "function"),
        (DependencyNode, "dependency"),
        (IdentityNode, "identity"),
        (DatabaseNode, "database"),
        (SchemaNode, "schema"),
        (TableNode, "table"),
        (ColumnNode, "column"),
        (BucketNode, "s3_bucket"),
        (ObjectPathNode, "s3_prefix"),
    ])
    def test_node_type(self, node_cls, expected_type):
        node = node_cls(
            organization_id=ORG_ID,
            urn=URN("urn:test:test:::test"),
        )
        assert node.node_type == expected_type


class TestCodebaseNodeCreate:
    def test_create_sets_metadata(self):
        node = CodebaseNode.create(
            ORG_ID, URN("urn:local:codebase:::myrepo"),
            repo_name="myrepo", file_count=42,
        )
        assert isinstance(node, CodebaseNode)
        assert isinstance(node, Node)
        assert node.node_type == "codebase"
        assert node.metadata[NK.REPO_NAME] == "myrepo"
        assert node.metadata[NK.FILE_COUNT] == 42

    def test_create_optional_fields(self):
        node = CodebaseNode.create(
            ORG_ID, URN("urn:local:codebase:::myrepo"),
            repo_name="myrepo",
        )
        assert NK.FILE_COUNT not in node.metadata


class TestFileNodeCreate:
    def test_create_sets_metadata(self):
        node = FileNode.create(
            ORG_ID, URN("urn:local:codebase:::myrepo/main.py"),
            URN("urn:local:codebase:::myrepo"),
            file_path="main.py", language="python", size_bytes=1024,
        )
        assert isinstance(node, FileNode)
        assert node.node_type == "file"
        assert node.metadata[NK.FILE_PATH] == "main.py"
        assert node.metadata[NK.LANGUAGE] == "python"
        assert node.metadata[NK.SIZE_BYTES] == 1024
        assert node.parent_urn == URN("urn:local:codebase:::myrepo")


class TestClassNodeCreate:
    def test_create_sets_metadata(self):
        node = ClassNode.create(
            ORG_ID, URN("urn:local:codebase:::myrepo/main.py/MyClass"),
            URN("urn:local:codebase:::myrepo/main.py"),
            class_name="MyClass", start_line=10, end_line=50,
            base_classes="Base,Mixin",
        )
        assert isinstance(node, ClassNode)
        assert node.node_type == "class"
        assert node.metadata[NK.CLASS_NAME] == "MyClass"
        assert node.metadata[NK.BASE_CLASSES] == "Base,Mixin"


class TestFunctionNodeCreate:
    def test_create_sets_metadata(self):
        node = FunctionNode.create(
            ORG_ID, URN("urn:local:codebase:::myrepo/main.py/my_func"),
            URN("urn:local:codebase:::myrepo/main.py"),
            function_name="my_func", start_line=1, end_line=10,
        )
        assert isinstance(node, FunctionNode)
        assert node.node_type == "function"
        assert node.metadata[NK.FUNCTION_NAME] == "my_func"
        assert node.metadata[NK.IS_METHOD] is False

    def test_create_method(self):
        node = FunctionNode.create(
            ORG_ID, URN("urn:local:codebase:::r/f.py/C/m"),
            URN("urn:local:codebase:::r/f.py/C"),
            function_name="m", start_line=5, end_line=15, is_method=True,
        )
        assert node.metadata[NK.IS_METHOD] is True


class TestDependencyNodeCreate:
    def test_create_sets_metadata(self):
        node = DependencyNode.create(
            ORG_ID, URN("urn:pypi:package:::requests"),
            package_name="requests", package_version="2.31.0",
            package_ecosystem="PyPI",
        )
        assert isinstance(node, DependencyNode)
        assert node.node_type == "dependency"
        assert node.metadata[NK.PACKAGE_NAME] == "requests"
        assert node.metadata[NK.PACKAGE_VERSION] == "2.31.0"


class TestIdentityNodeCreate:
    def test_create_sets_metadata(self):
        node = IdentityNode.create(
            ORG_ID, URN("urn:aws:rds:123:us-east-1:db/__roles/admin"),
            role_name="admin", role_login=True, role_superuser=False,
        )
        assert isinstance(node, IdentityNode)
        assert node.node_type == "identity"
        assert node.metadata[NK.ROLE_NAME] == "admin"
        assert node.metadata[NK.ROLE_LOGIN] is True


class TestDatabaseNodeCreate:
    def test_create_sets_metadata(self):
        node = DatabaseNode.create(
            ORG_ID, URN("urn:aws:rds:123:us-east-1:mydb"),
            database_name="mydb", host="localhost", port=5432,
        )
        assert isinstance(node, DatabaseNode)
        assert node.node_type == "database"
        assert node.metadata[NK.DATABASE_NAME] == "mydb"


class TestSchemaNodeCreate:
    def test_create_sets_metadata(self):
        node = SchemaNode.create(
            ORG_ID, URN("urn:aws:rds:123:us-east-1:mydb/public"),
            schema_name="public",
        )
        assert isinstance(node, SchemaNode)
        assert node.node_type == "schema"
        assert node.metadata[NK.SCHEMA_NAME] == "public"


class TestTableNodeCreate:
    def test_create_sets_metadata(self):
        node = TableNode.create(
            ORG_ID, URN("urn:aws:rds:123:us-east-1:mydb/public/users"),
            table_name="users", table_type="BASE TABLE",
        )
        assert isinstance(node, TableNode)
        assert node.node_type == "table"
        assert node.metadata[NK.TABLE_NAME] == "users"


class TestColumnNodeCreate:
    def test_create_sets_metadata(self):
        node = ColumnNode.create(
            ORG_ID,
            URN("urn:aws:rds:123:us-east-1:mydb/public/users/email"),
            column_name="email", data_type="varchar", nullable=False,
            ordinal_position=3,
        )
        assert isinstance(node, ColumnNode)
        assert node.node_type == "column"
        assert node.metadata[NK.COLUMN_NAME] == "email"
        assert node.metadata[NK.NULLABLE] is False


class TestBucketNodeCreate:
    def test_create_sets_metadata(self):
        node = BucketNode.create(
            ORG_ID, URN("urn:aws:s3:123:us-east-1:my-bucket"),
            bucket_name="my-bucket", arn="arn:aws:s3:::my-bucket",
            region="us-east-1",
        )
        assert isinstance(node, BucketNode)
        assert node.node_type == "s3_bucket"
        assert node.metadata[NK.BUCKET_NAME] == "my-bucket"


class TestObjectPathNodeCreate:
    def test_create_sets_metadata(self):
        node = ObjectPathNode.create(
            ORG_ID, URN("urn:aws:s3:123:us-east-1:my-bucket/uploads/"),
            path_pattern="uploads/", object_count=100,
        )
        assert isinstance(node, ObjectPathNode)
        assert node.node_type == "s3_prefix"
        assert node.metadata[NK.PATH_PATTERN] == "uploads/"
        assert node.metadata[NK.OBJECT_COUNT] == 100


class TestNodesWorkInListOfNode:
    """Typed nodes must be usable in list[Node] contexts."""

    def test_mixed_list(self):
        nodes: list[Node] = [
            CodebaseNode.create(ORG_ID, URN("urn:local:codebase:::r"), repo_name="r"),
            FileNode.create(ORG_ID, URN("urn:local:codebase:::r/f"), URN("urn:local:codebase:::r"), file_path="f"),
            FunctionNode.create(ORG_ID, URN("urn:local:codebase:::r/f/fn"), URN("urn:local:codebase:::r/f"), function_name="fn", start_line=1, end_line=5),
        ]
        assert len(nodes) == 3
        assert all(isinstance(n, Node) for n in nodes)
