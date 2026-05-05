"""Tests for BackendGroupToEcsStitcher."""

import uuid

from labyrinth.graph.graph_models import URN, EdgeType, Graph, NodeMetadataKey
from labyrinth.graph.nodes.backend_group_node import BackendGroupNode
from labyrinth.graph.nodes.ecs_service_node import EcsServiceNode
from labyrinth.graph.stitchers.backend_group_to_ecs import BackendGroupToEcsStitcher

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey


class TestBackendGroupToEcs:
    def test_bg_to_ecs_via_target_group_arn(self):
        tg_arn = "arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/my-tg/abc123"
        bg = BackendGroupNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:elb:123:us-east-1:my-alb/bg/my-tg"),
            bg_name="my-tg", bg_port=8000, bg_protocol="HTTP", arn=tg_arn,
        )
        ecs_svc = EcsServiceNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:ecs:123:us-east-1:cluster/my-svc"),
            service_name="my-svc",
        )
        ecs_svc.metadata[NK.ECS_TARGET_GROUP_ARNS] = [tg_arn]
        graph = Graph(nodes=[bg, ecs_svc])
        result = BackendGroupToEcsStitcher().stitch(ORG_ID, graph, {})

        routes_to = [e for e in result.edges if e.edge_type == EdgeType.ROUTES_TO]
        assert len(routes_to) == 1
        assert str(routes_to[0].from_urn) == str(bg.urn)
        assert str(routes_to[0].to_urn) == str(ecs_svc.urn)
        assert routes_to[0].metadata["detection_method"] == "ecs_target_group_match"
        assert routes_to[0].metadata["confidence"] == 1.0

    def test_bg_to_ecs_no_match_without_metadata(self):
        bg = BackendGroupNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:elb:123:us-east-1:my-alb/bg/my-tg"),
            bg_name="my-tg",
            arn="arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/my-tg/abc",
        )
        ecs_svc = EcsServiceNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:ecs:123:us-east-1:cluster/my-svc"),
            service_name="my-svc",
        )
        graph = Graph(nodes=[bg, ecs_svc])
        result = BackendGroupToEcsStitcher().stitch(ORG_ID, graph, {})
        assert len([e for e in result.edges if e.edge_type == EdgeType.ROUTES_TO]) == 0
