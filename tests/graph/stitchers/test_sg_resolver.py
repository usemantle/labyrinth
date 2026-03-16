"""Tests for SecurityGroupResolver."""

import uuid

from src.graph.edges.protected_by_edge import ProtectedByEdge
from src.graph.graph_models import URN, EdgeType, Graph, NodeMetadataKey
from src.graph.nodes.ecs_service_node import EcsServiceNode
from src.graph.nodes.load_balancer_node import LoadBalancerNode
from src.graph.nodes.security_group_node import SecurityGroupNode
from src.graph.stitchers.sg_resolver import SecurityGroupResolver

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey


def _sg_node(sg_id: str, vpc_id: str = "vpc-abc123") -> SecurityGroupNode:
    return SecurityGroupNode.create(
        organization_id=ORG_ID,
        urn=URN(f"urn:aws:vpc:123:us-east-1:{vpc_id}/sg/{sg_id}"),
        sg_id=sg_id,
        sg_name=f"test-{sg_id}",
        vpc_id=vpc_id,
    )


def _placeholder_protected_by(from_urn: URN, sg_id: str) -> ProtectedByEdge:
    return ProtectedByEdge.create(
        ORG_ID,
        from_urn,
        URN(f"urn:aws:vpc:123:us-east-1:unknown/sg/{sg_id}"),
    )


class TestResolveSecurityGroupUrns:
    def test_ecs_service_protected_by_resolved(self):
        sg = _sg_node("sg-ecs111")
        svc = EcsServiceNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:ecs:123:us-east-1:prod/api"),
            service_name="api",
        )
        edge = _placeholder_protected_by(svc.urn, "sg-ecs111")
        graph = Graph(nodes=[svc, sg], edges=[edge])

        result = SecurityGroupResolver().resolve(ORG_ID, graph, {})
        protected_by = [e for e in result.edges if e.edge_type == EdgeType.PROTECTED_BY]
        assert len(protected_by) == 1
        assert str(protected_by[0].from_urn) == str(svc.urn)
        assert str(protected_by[0].to_urn) == str(sg.urn)
        assert all(":unknown/sg/" not in str(e.to_urn) for e in result.edges)

    def test_alb_protected_by_resolved(self):
        sg = _sg_node("sg-alb222")
        lb = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:elb:123:us-east-1:my-alb"),
            lb_type="alb",
            lb_scheme="internet-facing",
        )
        edge = _placeholder_protected_by(lb.urn, "sg-alb222")
        graph = Graph(nodes=[lb, sg], edges=[edge])

        result = SecurityGroupResolver().resolve(ORG_ID, graph, {})
        protected_by = [e for e in result.edges if e.edge_type == EdgeType.PROTECTED_BY]
        assert len(protected_by) == 1
        assert str(protected_by[0].to_urn) == str(sg.urn)

    def test_multiple_sgs_resolved(self):
        sg1 = _sg_node("sg-aaa")
        sg2 = _sg_node("sg-bbb")
        svc = EcsServiceNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:ecs:123:us-east-1:prod/api"),
            service_name="api",
        )
        edge1 = _placeholder_protected_by(svc.urn, "sg-aaa")
        edge2 = _placeholder_protected_by(svc.urn, "sg-bbb")
        graph = Graph(nodes=[svc, sg1, sg2], edges=[edge1, edge2])

        result = SecurityGroupResolver().resolve(ORG_ID, graph, {})
        protected_by = [e for e in result.edges if e.edge_type == EdgeType.PROTECTED_BY]
        assert len(protected_by) == 2
        resolved_targets = {str(e.to_urn) for e in protected_by}
        assert str(sg1.urn) in resolved_targets
        assert str(sg2.urn) in resolved_targets

    def test_no_match_leaves_edge_unchanged(self):
        svc = EcsServiceNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:ecs:123:us-east-1:prod/api"),
            service_name="api",
        )
        edge = _placeholder_protected_by(svc.urn, "sg-orphan")
        graph = Graph(nodes=[svc], edges=[edge])

        result = SecurityGroupResolver().resolve(ORG_ID, graph, {})
        protected_by = [e for e in result.edges if e.edge_type == EdgeType.PROTECTED_BY]
        assert len(protected_by) == 1
        assert ":unknown/sg/" in str(protected_by[0].to_urn)

    def test_real_sg_urn_not_touched(self):
        sg = _sg_node("sg-real")
        svc = EcsServiceNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:ecs:123:us-east-1:prod/api"),
            service_name="api",
        )
        edge = ProtectedByEdge.create(ORG_ID, svc.urn, sg.urn)
        graph = Graph(nodes=[svc, sg], edges=[edge])

        result = SecurityGroupResolver().resolve(ORG_ID, graph, {})
        protected_by = [e for e in result.edges if e.edge_type == EdgeType.PROTECTED_BY]
        assert len(protected_by) == 1
        assert str(protected_by[0].to_urn) == str(sg.urn)
