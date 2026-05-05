"""Tests for AWS edge types."""

from __future__ import annotations

import uuid

from labyrinth.graph.graph_models import URN, EdgeMetadata, EdgeMetadataKey
from labyrinth.graph.edges.assumes_edge import AssumesEdge
from labyrinth.graph.edges.attaches_edge import AttachesEdge
from labyrinth.graph.edges.protected_by_edge import ProtectedByEdge
from labyrinth.graph.edges.allows_traffic_to_edge import AllowsTrafficToEdge
from labyrinth.graph.edges.member_of_edge import MemberOfEdge

ORG_ID = uuid.uuid4()
EK = EdgeMetadataKey


class TestAssumesEdge:
    def test_create(self):
        from_urn = URN("urn:aws:ecs:123:us-east-1:taskdef/my-task:1")
        to_urn = URN("urn:aws:iam:123::role/my-role")
        edge = AssumesEdge.create(ORG_ID, from_urn, to_urn)
        assert edge.edge_type == "assumes"
        assert edge.from_urn == from_urn
        assert edge.to_urn == to_urn

    def test_create_with_metadata(self):
        from_urn = URN("urn:aws:ecs:123:us-east-1:taskdef/my-task:1")
        to_urn = URN("urn:aws:iam:123::role/my-role")
        edge = AssumesEdge.create(
            ORG_ID, from_urn, to_urn,
            metadata=EdgeMetadata({EK.ASSUMED_VIA: "taskRoleArn"}),
        )
        assert edge.metadata[EK.ASSUMED_VIA] == "taskRoleArn"

    def test_deterministic_uuid(self):
        from_urn = URN("urn:aws:ecs:123:us-east-1:taskdef/my-task:1")
        to_urn = URN("urn:aws:iam:123::role/my-role")
        e1 = AssumesEdge.create(ORG_ID, from_urn, to_urn)
        e2 = AssumesEdge.create(ORG_ID, from_urn, to_urn)
        assert e1.uuid == e2.uuid


class TestAttachesEdge:
    def test_create(self):
        from_urn = URN("urn:aws:iam:123::policy/AdminAccess")
        to_urn = URN("urn:aws:iam:123::role/my-role")
        edge = AttachesEdge.create(ORG_ID, from_urn, to_urn)
        assert edge.edge_type == "attaches"


class TestProtectedByEdge:
    def test_create(self):
        from_urn = URN("urn:aws:rds:123:us-east-1:my-db")
        to_urn = URN("urn:aws:vpc:123:us-east-1:vpc-123/sg/sg-abc")
        edge = ProtectedByEdge.create(ORG_ID, from_urn, to_urn)
        assert edge.edge_type == "protected_by"


class TestAllowsTrafficToEdge:
    def test_create_with_metadata(self):
        from_urn = URN("urn:aws:vpc:123:us-east-1:vpc-123/sg/sg-frontend")
        to_urn = URN("urn:aws:vpc:123:us-east-1:vpc-123/sg/sg-database")
        edge = AllowsTrafficToEdge.create(
            ORG_ID, from_urn, to_urn,
            metadata=EdgeMetadata({
                EK.SG_RULE_PROTOCOL: "tcp",
                EK.SG_RULE_PORT_RANGE: "5432",
                EK.SG_RULE_DIRECTION: "ingress",
            }),
        )
        assert edge.edge_type == "allows_traffic_to"
        assert edge.metadata[EK.SG_RULE_PROTOCOL] == "tcp"


class TestMemberOfEdge:
    def test_create(self):
        from_urn = URN("urn:aws:sso:123::group/g-abc")
        to_urn = URN("urn:aws:iam:123::role/dev-role")
        edge = MemberOfEdge.create(ORG_ID, from_urn, to_urn)
        assert edge.edge_type == "member_of"
