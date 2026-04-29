"""Tests for StsAssumeRoleRelationsStitcher."""

from __future__ import annotations

import uuid

from src.graph.graph_models import (
    URN,
    EdgeMetadataKey,
    EdgeType,
    Graph,
    NodeMetadataKey,
)
from src.graph.nodes.iam_role_node import IamRoleNode
from src.graph.stitchers.sts_assume_role_relations import StsAssumeRoleRelationsStitcher

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey
EK = EdgeMetadataKey


def _role(account_id: str, role_name: str, *, trust_policy: dict | None = None) -> IamRoleNode:
    arn = f"arn:aws:iam::{account_id}:role/{role_name}"
    return IamRoleNode.create(
        ORG_ID,
        URN(f"urn:aws:iam:{account_id}::role/{role_name}"),
        role_name=role_name,
        trust_policy=trust_policy,
        arn=arn,
    )


def _trust(*, allow_principals: list[str] | str | None = None,
           action: str = "sts:AssumeRole",
           condition: dict | None = None) -> dict:
    statement: dict = {
        "Effect": "Allow",
        "Action": action,
        "Principal": {"AWS": allow_principals} if allow_principals is not None else "*",
    }
    if condition is not None:
        statement["Condition"] = condition
    return {"Version": "2012-10-17", "Statement": [statement]}


class TestStsAssumeRoleRelations:
    def test_role_to_role_via_explicit_arn(self):
        a = _role("111111111111", "RoleA")
        b = _role(
            "222222222222", "RoleB",
            trust_policy=_trust(
                allow_principals=["arn:aws:iam::111111111111:role/RoleA"],
            ),
        )
        graph = Graph(nodes=[a, b])

        result = StsAssumeRoleRelationsStitcher().stitch(ORG_ID, graph, {})

        edges = [e for e in result.edges if e.edge_type == EdgeType.ASSUMES]
        assert len(edges) == 1
        edge = edges[0]
        assert str(edge.from_urn) == str(a.urn)
        assert str(edge.to_urn) == str(b.urn)
        assert edge.metadata[EK.ASSUMED_VIA] == "iam:trust_policy"

    def test_wildcard_principal_skipped(self):
        a = _role("111111111111", "RoleA")
        b = _role(
            "222222222222", "RoleB",
            trust_policy={
                "Statement": [{
                    "Effect": "Allow",
                    "Action": "sts:AssumeRole",
                    "Principal": {"AWS": "*"},
                }],
            },
        )
        graph = Graph(nodes=[a, b])
        result = StsAssumeRoleRelationsStitcher().stitch(ORG_ID, graph, {})
        assert len(result.edges) == 0

    def test_account_root_principal_skipped(self):
        a = _role("111111111111", "RoleA")
        b = _role(
            "222222222222", "RoleB",
            trust_policy=_trust(
                allow_principals=["arn:aws:iam::111111111111:root"],
            ),
        )
        graph = Graph(nodes=[a, b])
        result = StsAssumeRoleRelationsStitcher().stitch(ORG_ID, graph, {})
        assert len(result.edges) == 0

    def test_service_principal_does_not_emit_edge(self):
        b = _role(
            "222222222222", "RoleB",
            trust_policy={
                "Statement": [{
                    "Effect": "Allow",
                    "Action": "sts:AssumeRole",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                }],
            },
        )
        graph = Graph(nodes=[b])
        result = StsAssumeRoleRelationsStitcher().stitch(ORG_ID, graph, {})
        assert len(result.edges) == 0

    def test_self_trust_skipped(self):
        a = _role(
            "111111111111", "RoleA",
            trust_policy=_trust(
                allow_principals=["arn:aws:iam::111111111111:role/RoleA"],
            ),
        )
        graph = Graph(nodes=[a])
        result = StsAssumeRoleRelationsStitcher().stitch(ORG_ID, graph, {})
        assert len(result.edges) == 0

    def test_unknown_arn_principal_skipped(self):
        a = _role("111111111111", "RoleA")
        b = _role(
            "222222222222", "RoleB",
            trust_policy=_trust(
                allow_principals=["arn:aws:iam::999999999999:role/Unknown"],
            ),
        )
        graph = Graph(nodes=[a, b])
        result = StsAssumeRoleRelationsStitcher().stitch(ORG_ID, graph, {})
        assert len(result.edges) == 0

    def test_multiple_principals_emit_multiple_edges(self):
        a = _role("111111111111", "RoleA")
        c = _role("333333333333", "RoleC")
        b = _role(
            "222222222222", "RoleB",
            trust_policy=_trust(
                allow_principals=[
                    "arn:aws:iam::111111111111:role/RoleA",
                    "arn:aws:iam::333333333333:role/RoleC",
                ],
            ),
        )
        graph = Graph(nodes=[a, b, c])
        result = StsAssumeRoleRelationsStitcher().stitch(ORG_ID, graph, {})
        edges = [e for e in result.edges if e.edge_type == EdgeType.ASSUMES]
        assert len(edges) == 2
        sources = {str(e.from_urn) for e in edges}
        assert sources == {str(a.urn), str(c.urn)}

    def test_condition_preserved_in_edge_metadata(self):
        a = _role("111111111111", "RoleA")
        condition = {"StringEquals": {"sts:ExternalId": "secret"}}
        b = _role(
            "222222222222", "RoleB",
            trust_policy=_trust(
                allow_principals=["arn:aws:iam::111111111111:role/RoleA"],
                condition=condition,
            ),
        )
        graph = Graph(nodes=[a, b])
        result = StsAssumeRoleRelationsStitcher().stitch(ORG_ID, graph, {})
        edges = [e for e in result.edges if e.edge_type == EdgeType.ASSUMES]
        assert len(edges) == 1
        assert edges[0].metadata[EK.TRUST_POLICY_CONDITION] == condition

    def test_deny_statement_skipped(self):
        a = _role("111111111111", "RoleA")
        b = _role(
            "222222222222", "RoleB",
            trust_policy={
                "Statement": [{
                    "Effect": "Deny",
                    "Action": "sts:AssumeRole",
                    "Principal": {
                        "AWS": "arn:aws:iam::111111111111:role/RoleA",
                    },
                }],
            },
        )
        graph = Graph(nodes=[a, b])
        result = StsAssumeRoleRelationsStitcher().stitch(ORG_ID, graph, {})
        assert len(result.edges) == 0

    def test_no_iam_roles_returns_empty(self):
        result = StsAssumeRoleRelationsStitcher().stitch(ORG_ID, Graph(), {})
        assert len(result.edges) == 0

    def test_role_without_trust_policy_skipped(self):
        a = _role("111111111111", "RoleA")
        b = _role("222222222222", "RoleB", trust_policy=None)
        graph = Graph(nodes=[a, b])
        result = StsAssumeRoleRelationsStitcher().stitch(ORG_ID, graph, {})
        assert len(result.edges) == 0
