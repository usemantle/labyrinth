"""Tests for stitch_aws_resources — security group URN resolution."""

import uuid

from src.graph.edges.protected_by_edge import ProtectedByEdge
from src.graph.graph_models import URN, EdgeType, NodeMetadataKey
from src.graph.nodes.ecs_service_node import EcsServiceNode
from src.graph.nodes.load_balancer_node import LoadBalancerNode
from src.graph.nodes.security_group_node import SecurityGroupNode
from src.graph.stitching import stitch_aws_resources

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
    """Create a ProtectedByEdge with the 'unknown' VPC placeholder URN."""
    return ProtectedByEdge.create(
        ORG_ID,
        from_urn,
        URN(f"urn:aws:vpc:123:us-east-1:unknown/sg/{sg_id}"),
    )


class TestResolveSecurityGroupUrns:
    def test_ecs_service_protected_by_resolved(self):
        """ECS service -> unknown SG URN is rewritten to the real SG URN."""
        sg = _sg_node("sg-ecs111")
        svc = EcsServiceNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:ecs:123:us-east-1:prod/api"),
            service_name="api",
        )
        edge = _placeholder_protected_by(svc.urn, "sg-ecs111")

        nodes = [svc, sg]
        edges = [edge]
        _, result_edges = stitch_aws_resources(ORG_ID, nodes, edges)

        protected_by = [e for e in result_edges if e.edge_type == EdgeType.PROTECTED_BY]
        assert len(protected_by) == 1
        assert str(protected_by[0].from_urn) == str(svc.urn)
        assert str(protected_by[0].to_urn) == str(sg.urn)
        # Old placeholder edge should be gone
        assert all(":unknown/sg/" not in str(e.to_urn) for e in result_edges)

    def test_alb_protected_by_resolved(self):
        """ALB -> unknown SG URN is rewritten to the real SG URN."""
        sg = _sg_node("sg-alb222")
        lb = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:elb:123:us-east-1:my-alb"),
            lb_type="alb",
            lb_scheme="internet-facing",
        )
        edge = _placeholder_protected_by(lb.urn, "sg-alb222")

        nodes = [lb, sg]
        edges = [edge]
        _, result_edges = stitch_aws_resources(ORG_ID, nodes, edges)

        protected_by = [e for e in result_edges if e.edge_type == EdgeType.PROTECTED_BY]
        assert len(protected_by) == 1
        assert str(protected_by[0].to_urn) == str(sg.urn)

    def test_multiple_sgs_resolved(self):
        """Multiple placeholder edges are all resolved."""
        sg1 = _sg_node("sg-aaa")
        sg2 = _sg_node("sg-bbb")
        svc = EcsServiceNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:ecs:123:us-east-1:prod/api"),
            service_name="api",
        )
        edge1 = _placeholder_protected_by(svc.urn, "sg-aaa")
        edge2 = _placeholder_protected_by(svc.urn, "sg-bbb")

        nodes = [svc, sg1, sg2]
        edges = [edge1, edge2]
        _, result_edges = stitch_aws_resources(ORG_ID, nodes, edges)

        protected_by = [e for e in result_edges if e.edge_type == EdgeType.PROTECTED_BY]
        assert len(protected_by) == 2
        resolved_targets = {str(e.to_urn) for e in protected_by}
        assert str(sg1.urn) in resolved_targets
        assert str(sg2.urn) in resolved_targets

    def test_no_match_leaves_edge_unchanged(self):
        """Placeholder edge with no matching SG node is left as-is."""
        svc = EcsServiceNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:ecs:123:us-east-1:prod/api"),
            service_name="api",
        )
        edge = _placeholder_protected_by(svc.urn, "sg-orphan")

        nodes = [svc]  # no SG node
        edges = [edge]
        _, result_edges = stitch_aws_resources(ORG_ID, nodes, edges)

        protected_by = [e for e in result_edges if e.edge_type == EdgeType.PROTECTED_BY]
        assert len(protected_by) == 1
        assert ":unknown/sg/" in str(protected_by[0].to_urn)

    def test_real_sg_urn_not_touched(self):
        """ProtectedByEdge already pointing to a real SG URN is not modified."""
        sg = _sg_node("sg-real")
        svc = EcsServiceNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:ecs:123:us-east-1:prod/api"),
            service_name="api",
        )
        # Edge already has the correct URN (e.g., from RDS plugin which knows the VPC)
        edge = ProtectedByEdge.create(ORG_ID, svc.urn, sg.urn)

        nodes = [svc, sg]
        edges = [edge]
        _, result_edges = stitch_aws_resources(ORG_ID, nodes, edges)

        protected_by = [e for e in result_edges if e.edge_type == EdgeType.PROTECTED_BY]
        assert len(protected_by) == 1
        assert str(protected_by[0].to_urn) == str(sg.urn)
