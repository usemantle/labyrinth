"""Tests for edge relationship validation."""

import uuid

from src.graph.edges import (
    CallsEdge,
    ContainsEdge,
    DependsOnEdge,
    HostsEdge,
    InstantiatesEdge,
    ModelsEdge,
    ReadsEdge,
    ReferencesEdge,
    SoftReferenceEdge,
    WritesEdge,
    validate_edge,
)
from src.graph.graph_models import URN, Node
from src.graph.nodes import (
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

ORG_ID = uuid.uuid4()


def _urn(path: str) -> URN:
    return URN(f"urn:test:test:::{path}")


class TestValidEdges:
    """Valid edge-node combinations must pass validation."""

    def test_codebase_contains_file(self):
        cb = CodebaseNode.create(ORG_ID, _urn("repo"), repo_name="repo")
        f = FileNode.create(ORG_ID, _urn("repo/f.py"), _urn("repo"), file_path="f.py")
        edge = ContainsEdge.create(ORG_ID, cb.urn, f.urn)
        assert validate_edge(edge, cb, f) == []

    def test_file_contains_function(self):
        f = FileNode.create(ORG_ID, _urn("r/f.py"), _urn("r"), file_path="f.py")
        fn = FunctionNode.create(ORG_ID, _urn("r/f.py/fn"), _urn("r/f.py"), function_name="fn", start_line=1, end_line=5)
        edge = ContainsEdge.create(ORG_ID, f.urn, fn.urn)
        assert validate_edge(edge, f, fn) == []

    def test_file_contains_class(self):
        f = FileNode.create(ORG_ID, _urn("r/f.py"), _urn("r"), file_path="f.py")
        c = ClassNode.create(ORG_ID, _urn("r/f.py/C"), _urn("r/f.py"), class_name="C", start_line=1, end_line=50)
        edge = ContainsEdge.create(ORG_ID, f.urn, c.urn)
        assert validate_edge(edge, f, c) == []

    def test_class_contains_method(self):
        c = ClassNode.create(ORG_ID, _urn("r/f.py/C"), _urn("r/f.py"), class_name="C", start_line=1, end_line=50)
        m = FunctionNode.create(ORG_ID, _urn("r/f.py/C/m"), _urn("r/f.py/C"), function_name="m", start_line=5, end_line=15, is_method=True)
        edge = ContainsEdge.create(ORG_ID, c.urn, m.urn)
        assert validate_edge(edge, c, m) == []

    def test_function_calls_function(self):
        fn1 = FunctionNode.create(ORG_ID, _urn("r/f.py/a"), _urn("r/f.py"), function_name="a", start_line=1, end_line=5)
        fn2 = FunctionNode.create(ORG_ID, _urn("r/f.py/b"), _urn("r/f.py"), function_name="b", start_line=10, end_line=15)
        edge = CallsEdge.create(ORG_ID, fn1.urn, fn2.urn)
        assert validate_edge(edge, fn1, fn2) == []

    def test_function_reads_table(self):
        fn = FunctionNode.create(ORG_ID, _urn("r/f.py/fn"), _urn("r/f.py"), function_name="fn", start_line=1, end_line=5)
        t = TableNode.create(ORG_ID, _urn("db/public/users"), table_name="users")
        edge = ReadsEdge.create(ORG_ID, fn.urn, t.urn)
        assert validate_edge(edge, fn, t) == []

    def test_function_writes_table(self):
        fn = FunctionNode.create(ORG_ID, _urn("r/f.py/fn"), _urn("r/f.py"), function_name="fn", start_line=1, end_line=5)
        t = TableNode.create(ORG_ID, _urn("db/public/users"), table_name="users")
        edge = WritesEdge.create(ORG_ID, fn.urn, t.urn)
        assert validate_edge(edge, fn, t) == []

    def test_class_models_table(self):
        c = ClassNode.create(ORG_ID, _urn("r/f.py/User"), _urn("r/f.py"), class_name="User", start_line=1, end_line=50)
        t = TableNode.create(ORG_ID, _urn("db/public/users"), table_name="users")
        edge = ModelsEdge.create(ORG_ID, c.urn, t.urn)
        assert validate_edge(edge, c, t) == []

    def test_column_references_column(self):
        c1 = ColumnNode.create(ORG_ID, _urn("db/public/orders/customer_id"), column_name="customer_id")
        c2 = ColumnNode.create(ORG_ID, _urn("db/public/customers/id"), column_name="id")
        edge = ReferencesEdge.create(ORG_ID, c1.urn, c2.urn)
        assert validate_edge(edge, c1, c2) == []

    def test_column_soft_references_column(self):
        c1 = ColumnNode.create(ORG_ID, _urn("db/public/orders/customer_id"), column_name="customer_id")
        c2 = ColumnNode.create(ORG_ID, _urn("db/public/customers/id"), column_name="id")
        edge = SoftReferenceEdge.create(ORG_ID, c1.urn, c2.urn)
        assert validate_edge(edge, c1, c2) == []

    def test_dependency_depends_on_dependency(self):
        d1 = DependencyNode.create(ORG_ID, _urn("dep/requests"), package_name="requests")
        d2 = DependencyNode.create(ORG_ID, _urn("dep/urllib3"), package_name="urllib3")
        edge = DependsOnEdge.create(ORG_ID, d1.urn, d2.urn)
        assert validate_edge(edge, d1, d2) == []

    def test_identity_reads_table(self):
        role = IdentityNode.create(ORG_ID, _urn("db/__roles/reader"), role_name="reader")
        t = TableNode.create(ORG_ID, _urn("db/public/users"), table_name="users")
        edge = ReadsEdge.create(ORG_ID, role.urn, t.urn)
        assert validate_edge(edge, role, t) == []

    def test_identity_writes_table(self):
        role = IdentityNode.create(ORG_ID, _urn("db/__roles/writer"), role_name="writer")
        t = TableNode.create(ORG_ID, _urn("db/public/users"), table_name="users")
        edge = WritesEdge.create(ORG_ID, role.urn, t.urn)
        assert validate_edge(edge, role, t) == []

    def test_hosts_edge_to_database(self):
        infra = Node(organization_id=ORG_ID, urn=_urn("rds-cluster"))
        db = DatabaseNode.create(ORG_ID, _urn("db"), database_name="mydb")
        edge = HostsEdge.create(ORG_ID, infra.urn, db.urn)
        # infra is bare Node so outgoing check is skipped, only incoming check on db
        assert validate_edge(edge, infra, db) == []

    def test_database_contains_schema(self):
        db = DatabaseNode.create(ORG_ID, _urn("db"), database_name="mydb")
        s = SchemaNode.create(ORG_ID, _urn("db/public"), schema_name="public")
        edge = ContainsEdge.create(ORG_ID, db.urn, s.urn)
        assert validate_edge(edge, db, s) == []

    def test_schema_contains_table(self):
        s = SchemaNode.create(ORG_ID, _urn("db/public"), schema_name="public")
        t = TableNode.create(ORG_ID, _urn("db/public/users"), table_name="users")
        edge = ContainsEdge.create(ORG_ID, s.urn, t.urn)
        assert validate_edge(edge, s, t) == []

    def test_table_contains_column(self):
        t = TableNode.create(ORG_ID, _urn("db/public/users"), table_name="users")
        c = ColumnNode.create(ORG_ID, _urn("db/public/users/email"), column_name="email")
        edge = ContainsEdge.create(ORG_ID, t.urn, c.urn)
        assert validate_edge(edge, t, c) == []

    def test_bucket_contains_path(self):
        b = BucketNode.create(ORG_ID, _urn("my-bucket"), bucket_name="my-bucket")
        p = ObjectPathNode.create(ORG_ID, _urn("my-bucket/uploads/"), path_pattern="uploads/")
        edge = ContainsEdge.create(ORG_ID, b.urn, p.urn)
        assert validate_edge(edge, b, p) == []

    def test_function_reads_object_path(self):
        fn = FunctionNode.create(ORG_ID, _urn("r/f.py/fn"), _urn("r/f.py"), function_name="fn", start_line=1, end_line=5)
        p = ObjectPathNode.create(ORG_ID, _urn("bucket/uploads/"), path_pattern="uploads/")
        edge = ReadsEdge.create(ORG_ID, fn.urn, p.urn)
        assert validate_edge(edge, fn, p) == []

    def test_codebase_contains_dependency(self):
        cb = CodebaseNode.create(ORG_ID, _urn("repo"), repo_name="repo")
        dep = DependencyNode.create(ORG_ID, _urn("dep/requests"), package_name="requests")
        edge = ContainsEdge.create(ORG_ID, cb.urn, dep.urn)
        assert validate_edge(edge, cb, dep) == []

    def test_function_instantiates_class(self):
        fn = FunctionNode.create(ORG_ID, _urn("r/f.py/fn"), _urn("r/f.py"), function_name="fn", start_line=1, end_line=5)
        c = ClassNode.create(ORG_ID, _urn("r/f.py/C"), _urn("r/f.py"), class_name="C", start_line=10, end_line=50)
        edge = InstantiatesEdge.create(ORG_ID, fn.urn, c.urn)
        assert validate_edge(edge, fn, c) == []

    def test_file_depends_on_dependency(self):
        f = FileNode.create(ORG_ID, _urn("r/f.py"), _urn("r"), file_path="f.py")
        dep = DependencyNode.create(ORG_ID, _urn("dep/requests"), package_name="requests")
        edge = DependsOnEdge.create(ORG_ID, f.urn, dep.urn)
        assert validate_edge(edge, f, dep) == []


class TestInvalidEdges:
    """Invalid edge-node combinations must produce violations."""

    def test_codebase_cannot_call(self):
        cb = CodebaseNode.create(ORG_ID, _urn("repo"), repo_name="repo")
        fn = FunctionNode.create(ORG_ID, _urn("r/f.py/fn"), _urn("r/f.py"), function_name="fn", start_line=1, end_line=5)
        edge = CallsEdge.create(ORG_ID, cb.urn, fn.urn)
        violations = validate_edge(edge, cb, fn)
        assert len(violations) > 0
        assert "CodebaseNode" in violations[0]
        assert "CallsEdge" in violations[0]

    def test_column_cannot_contain(self):
        c = ColumnNode.create(ORG_ID, _urn("db/public/users/email"), column_name="email")
        t = TableNode.create(ORG_ID, _urn("db/public/users"), table_name="users")
        edge = ContainsEdge.create(ORG_ID, c.urn, t.urn)
        violations = validate_edge(edge, c, t)
        assert len(violations) > 0

    def test_identity_cannot_call(self):
        role = IdentityNode.create(ORG_ID, _urn("db/__roles/admin"), role_name="admin")
        fn = FunctionNode.create(ORG_ID, _urn("r/f.py/fn"), _urn("r/f.py"), function_name="fn", start_line=1, end_line=5)
        edge = CallsEdge.create(ORG_ID, role.urn, fn.urn)
        violations = validate_edge(edge, role, fn)
        assert len(violations) > 0

    def test_function_cannot_receive_reads(self):
        fn1 = FunctionNode.create(ORG_ID, _urn("r/f.py/a"), _urn("r/f.py"), function_name="a", start_line=1, end_line=5)
        fn2 = FunctionNode.create(ORG_ID, _urn("r/f.py/b"), _urn("r/f.py"), function_name="b", start_line=10, end_line=15)
        edge = ReadsEdge.create(ORG_ID, fn1.urn, fn2.urn)
        violations = validate_edge(edge, fn1, fn2)
        # fn1 can send ReadsEdge (outgoing ok), but fn2 cannot receive ReadsEdge (incoming violation)
        assert any("FunctionNode" in v and "incoming" in v for v in violations)

    def test_table_cannot_send_reads(self):
        t = TableNode.create(ORG_ID, _urn("db/public/users"), table_name="users")
        fn = FunctionNode.create(ORG_ID, _urn("r/f.py/fn"), _urn("r/f.py"), function_name="fn", start_line=1, end_line=5)
        edge = ReadsEdge.create(ORG_ID, t.urn, fn.urn)
        violations = validate_edge(edge, t, fn)
        assert len(violations) > 0

    def test_bucket_cannot_receive_calls(self):
        b = BucketNode.create(ORG_ID, _urn("bucket"), bucket_name="bucket")
        fn = FunctionNode.create(ORG_ID, _urn("r/f.py/fn"), _urn("r/f.py"), function_name="fn", start_line=1, end_line=5)
        edge = CallsEdge.create(ORG_ID, fn.urn, b.urn)
        violations = validate_edge(edge, fn, b)
        assert any("BucketNode" in v for v in violations)

    def test_class_cannot_send_instantiates(self):
        c = ClassNode.create(ORG_ID, _urn("r/f.py/C"), _urn("r/f.py"), class_name="C", start_line=1, end_line=50)
        c2 = ClassNode.create(ORG_ID, _urn("r/f.py/D"), _urn("r/f.py"), class_name="D", start_line=55, end_line=100)
        edge = InstantiatesEdge.create(ORG_ID, c.urn, c2.urn)
        violations = validate_edge(edge, c, c2)
        assert len(violations) > 0


class TestBackwardCompat:
    """Bare Node instances skip validation."""

    def test_bare_nodes_skip_validation(self):
        n1 = Node(organization_id=ORG_ID, urn=_urn("a"))
        n2 = Node(organization_id=ORG_ID, urn=_urn("b"))
        edge = CallsEdge.create(ORG_ID, n1.urn, n2.urn)
        assert validate_edge(edge, n1, n2) == []

    def test_typed_from_bare_to(self):
        fn = FunctionNode.create(ORG_ID, _urn("r/f.py/fn"), _urn("r/f.py"), function_name="fn", start_line=1, end_line=5)
        bare = Node(organization_id=ORG_ID, urn=_urn("unknown"))
        edge = CallsEdge.create(ORG_ID, fn.urn, bare.urn)
        # outgoing from FunctionNode is valid, bare to_node is skipped
        assert validate_edge(edge, fn, bare) == []

    def test_bare_from_typed_to(self):
        bare = Node(organization_id=ORG_ID, urn=_urn("unknown"))
        fn = FunctionNode.create(ORG_ID, _urn("r/f.py/fn"), _urn("r/f.py"), function_name="fn", start_line=1, end_line=5)
        edge = ContainsEdge.create(ORG_ID, bare.urn, fn.urn)
        # bare from_node is skipped, incoming to FunctionNode allows ContainsEdge
        assert validate_edge(edge, bare, fn) == []
