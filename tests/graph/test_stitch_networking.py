"""Tests for networking topology stitching."""

import uuid

from src.graph.graph_models import URN, EdgeType, NodeMetadataKey
from src.graph.nodes.backend_group_node import BackendGroupNode
from src.graph.nodes.dns_record_node import DnsRecordNode
from src.graph.nodes.ecs_service_node import EcsServiceNode
from src.graph.nodes.load_balancer_node import LoadBalancerNode
from src.graph.stitching import stitch_networking

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey


class TestStitchNetworking:
    def test_dns_to_lb_alias_match(self):
        """DNS alias target matching LB dns_name creates a ResolvesToEdge."""
        lb = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:elb:123:us-east-1:my-alb"),
            lb_type="alb",
            lb_scheme="internet-facing",
            lb_dns_name="my-alb-123.us-east-1.elb.amazonaws.com",
        )
        dns = DnsRecordNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:route53:123::Z1/api.example.com/A"),
            record_name="api.example.com",
            record_type="A",
            zone_private=False,
            values=["my-alb-123.us-east-1.elb.amazonaws.com"],
        )

        nodes = [lb, dns]
        edges = []
        _, result_edges = stitch_networking(ORG_ID, nodes, edges)

        resolves_to = [e for e in result_edges if e.edge_type == EdgeType.RESOLVES_TO]
        assert len(resolves_to) == 1
        assert str(resolves_to[0].from_urn) == str(dns.urn)
        assert str(resolves_to[0].to_urn) == str(lb.urn)

    def test_dns_to_lb_case_insensitive(self):
        """DNS matching is case-insensitive and strips trailing dots."""
        lb = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:elb:123:us-east-1:my-alb"),
            lb_type="alb",
            lb_scheme="internet-facing",
            lb_dns_name="MY-ALB.us-east-1.elb.amazonaws.com",
        )
        dns = DnsRecordNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:route53:123::Z1/app.example.com/A"),
            record_name="app.example.com",
            record_type="A",
            values=["my-alb.us-east-1.elb.amazonaws.com."],
        )

        nodes = [lb, dns]
        edges = []
        _, result_edges = stitch_networking(ORG_ID, nodes, edges)

        resolves_to = [e for e in result_edges if e.edge_type == EdgeType.RESOLVES_TO]
        assert len(resolves_to) == 1

    def test_no_match_returns_no_edges(self):
        """No DNS values match any LB dns_name."""
        lb = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:elb:123:us-east-1:my-alb"),
            lb_type="alb",
            lb_scheme="internet-facing",
            lb_dns_name="my-alb.elb.amazonaws.com",
        )
        dns = DnsRecordNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:route53:123::Z1/unrelated.example.com/A"),
            record_name="unrelated.example.com",
            record_type="A",
            values=["1.2.3.4"],
        )

        nodes = [lb, dns]
        edges = []
        _, result_edges = stitch_networking(ORG_ID, nodes, edges)
        assert len(result_edges) == 0

    def test_dns_to_apigw_via_custom_domain(self):
        """DNS alias matching an API Gateway custom domain creates a ResolvesToEdge."""
        apigw = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:apigateway:123:us-east-1:abc123"),
            lb_type="api_gateway_http",
            lb_scheme="internet-facing",
            lb_dns_name="https://abc123.execute-api.us-east-1.amazonaws.com",
            api_gw_endpoint_type="HTTP",
        )
        # Add custom domain metadata
        apigw.metadata[NK.API_GW_CUSTOM_DOMAINS] = [
            "d-xyz789.execute-api.us-east-1.amazonaws.com",
            "api.example.com",
        ]

        dns = DnsRecordNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:route53:123::Z1/api.example.com/A"),
            record_name="api.example.com",
            record_type="A",
            values=["d-xyz789.execute-api.us-east-1.amazonaws.com"],
        )

        nodes = [apigw, dns]
        edges = []
        _, result_edges = stitch_networking(ORG_ID, nodes, edges)

        resolves_to = [e for e in result_edges if e.edge_type == EdgeType.RESOLVES_TO]
        assert len(resolves_to) == 1
        assert str(resolves_to[0].from_urn) == str(dns.urn)
        assert str(resolves_to[0].to_urn) == str(apigw.urn)

    def test_dns_to_apigw_strips_https_prefix(self):
        """LB dns_name with https:// prefix is normalized for matching."""
        apigw = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:apigateway:123:us-east-1:abc123"),
            lb_type="api_gateway_http",
            lb_scheme="internet-facing",
            lb_dns_name="https://abc123.execute-api.us-east-1.amazonaws.com",
        )
        dns = DnsRecordNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:route53:123::Z1/api.example.com/A"),
            record_name="api.example.com",
            record_type="A",
            values=["abc123.execute-api.us-east-1.amazonaws.com"],
        )

        nodes = [apigw, dns]
        edges = []
        _, result_edges = stitch_networking(ORG_ID, nodes, edges)

        resolves_to = [e for e in result_edges if e.edge_type == EdgeType.RESOLVES_TO]
        assert len(resolves_to) == 1

    def test_apigw_to_alb_via_integration_uri(self):
        """API GW with integration URI matching ALB listener ARN creates RoutesToEdge."""
        alb = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:elb:123:us-east-1:my-alb"),
            lb_type="alb",
            lb_scheme="internal",
            lb_dns_name="internal-my-alb-123.us-east-1.elb.amazonaws.com",
            listeners=[{"port": 80, "protocol": "HTTP"}],
            arn="arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/app/my-alb/abc123",
        )
        apigw = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:apigateway:123:us-east-1:api789"),
            lb_type="api_gateway_http",
            lb_scheme="internet-facing",
            lb_dns_name="https://api789.execute-api.us-east-1.amazonaws.com",
        )
        apigw.metadata[NK.API_GW_INTEGRATION_URIS] = [
            "arn:aws:elasticloadbalancing:us-east-1:123:listener/app/my-alb/abc123/def456",
        ]

        nodes = [alb, apigw]
        edges = []
        _, result_edges = stitch_networking(ORG_ID, nodes, edges)

        routes_to = [e for e in result_edges if e.edge_type == EdgeType.ROUTES_TO]
        assert len(routes_to) == 1
        assert str(routes_to[0].from_urn) == str(apigw.urn)
        assert str(routes_to[0].to_urn) == str(alb.urn)
        assert routes_to[0].metadata["detection_method"] == "apigw_integration_match"

    def test_apigw_no_match_without_integration(self):
        """API GW without integration URIs creates no RoutesToEdge."""
        alb = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:elb:123:us-east-1:my-alb"),
            lb_type="alb",
            lb_scheme="internal",
            lb_dns_name="my-alb-123.elb.amazonaws.com",
            arn="arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/app/my-alb/abc",
        )
        apigw = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:apigateway:123:us-east-1:api789"),
            lb_type="api_gateway_http",
            lb_scheme="internet-facing",
            lb_dns_name="https://api789.execute-api.us-east-1.amazonaws.com",
        )

        nodes = [alb, apigw]
        edges = []
        _, result_edges = stitch_networking(ORG_ID, nodes, edges)

        routes_to = [e for e in result_edges if e.edge_type == EdgeType.ROUTES_TO]
        assert len(routes_to) == 0

    def test_bg_to_ecs_via_target_group_arn(self):
        """Target group ARN on ECS service creates RoutesToEdge from BG to ECS."""
        tg_arn = "arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/my-tg/abc123"
        bg = BackendGroupNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:elb:123:us-east-1:my-alb/bg/my-tg"),
            bg_name="my-tg",
            bg_port=8000,
            bg_protocol="HTTP",
            arn=tg_arn,
        )
        ecs_svc = EcsServiceNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:ecs:123:us-east-1:cluster/my-svc"),
            service_name="my-svc",
        )
        ecs_svc.metadata[NK.ECS_TARGET_GROUP_ARNS] = [tg_arn]

        nodes = [bg, ecs_svc]
        edges = []
        _, result_edges = stitch_networking(ORG_ID, nodes, edges)

        routes_to = [e for e in result_edges if e.edge_type == EdgeType.ROUTES_TO]
        assert len(routes_to) == 1
        assert str(routes_to[0].from_urn) == str(bg.urn)
        assert str(routes_to[0].to_urn) == str(ecs_svc.urn)
        assert routes_to[0].metadata["detection_method"] == "ecs_target_group_match"
        assert routes_to[0].metadata["confidence"] == 1.0

    def test_bg_to_ecs_no_match_without_metadata(self):
        """ECS service without target group ARNs creates no RoutesToEdge."""
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

        nodes = [bg, ecs_svc]
        edges = []
        _, result_edges = stitch_networking(ORG_ID, nodes, edges)

        routes_to = [e for e in result_edges if e.edge_type == EdgeType.ROUTES_TO]
        assert len(routes_to) == 0

    def test_empty_graph(self):
        """Empty node list returns empty edges."""
        _, result_edges = stitch_networking(ORG_ID, [], [])
        assert len(result_edges) == 0
