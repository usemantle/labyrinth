"""Tests for ApiGwToAlbStitcher."""

import uuid

from labyrinth.graph.graph_models import URN, EdgeType, Graph, NodeMetadataKey
from labyrinth.graph.nodes.load_balancer_node import LoadBalancerNode
from labyrinth.graph.stitchers.apigw_to_alb import ApiGwToAlbStitcher

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey


class TestApiGwToAlb:
    def test_apigw_to_alb_via_integration_uri(self):
        alb = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:elb:123:us-east-1:my-alb"),
            lb_type="alb", lb_scheme="internal",
            lb_dns_name="internal-my-alb-123.us-east-1.elb.amazonaws.com",
            listeners=[{"port": 80, "protocol": "HTTP"}],
            arn="arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/app/my-alb/abc123",
        )
        apigw = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:apigateway:123:us-east-1:api789"),
            lb_type="api_gateway_http", lb_scheme="internet-facing",
            lb_dns_name="https://api789.execute-api.us-east-1.amazonaws.com",
        )
        apigw.metadata[NK.API_GW_INTEGRATION_URIS] = [
            "arn:aws:elasticloadbalancing:us-east-1:123:listener/app/my-alb/abc123/def456",
        ]
        graph = Graph(nodes=[alb, apigw])
        result = ApiGwToAlbStitcher().stitch(ORG_ID, graph, {})

        routes_to = [e for e in result.edges if e.edge_type == EdgeType.ROUTES_TO]
        assert len(routes_to) == 1
        assert str(routes_to[0].from_urn) == str(apigw.urn)
        assert str(routes_to[0].to_urn) == str(alb.urn)
        assert routes_to[0].metadata["detection_method"] == "apigw_integration_match"

    def test_apigw_no_match_without_integration(self):
        alb = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:elb:123:us-east-1:my-alb"),
            lb_type="alb", lb_scheme="internal",
            lb_dns_name="my-alb-123.elb.amazonaws.com",
            arn="arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/app/my-alb/abc",
        )
        apigw = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:apigateway:123:us-east-1:api789"),
            lb_type="api_gateway_http", lb_scheme="internet-facing",
            lb_dns_name="https://api789.execute-api.us-east-1.amazonaws.com",
        )
        graph = Graph(nodes=[alb, apigw])
        result = ApiGwToAlbStitcher().stitch(ORG_ID, graph, {})
        assert len([e for e in result.edges if e.edge_type == EdgeType.ROUTES_TO]) == 0
