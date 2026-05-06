"""Tests for DnsToLoadBalancerStitcher."""

import uuid

from labyrinth.graph.graph_models import EdgeType, Graph, NodeMetadataKey
from labyrinth.graph.nodes.api_gateway_node import ApiGatewayNode
from labyrinth.graph.nodes.dns_record_node import DnsRecordNode
from labyrinth.graph.nodes.load_balancer_node import LoadBalancerNode
from labyrinth.graph.stitchers.dns_to_load_balancer import DnsToLoadBalancerStitcher

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey


def _alb(name: str, dns: str, **extras) -> LoadBalancerNode:
    return LoadBalancerNode.create(
        organization_id=ORG_ID,
        urn=LoadBalancerNode.build_urn("123", "us-east-1", name),
        lb_type="alb", lb_scheme="internet-facing",
        lb_dns_name=dns,
        **extras,
    )


def _apigw(api_id: str, dns: str, **extras) -> ApiGatewayNode:
    return ApiGatewayNode.create(
        organization_id=ORG_ID,
        urn=ApiGatewayNode.build_urn("us-east-1", api_id),
        api_gw_type="http", scheme="internet-facing",
        dns_name=dns,
        **extras,
    )


def _dns(zone: str, record_name: str, values: list[str], **extras) -> DnsRecordNode:
    return DnsRecordNode.create(
        organization_id=ORG_ID,
        urn=DnsRecordNode.build_urn(zone, record_name, "A"),
        record_name=record_name, record_type="A",
        values=values, **extras,
    )


class TestDnsToLoadBalancer:
    def test_dns_to_lb_alias_match(self):
        lb = _alb("my-alb", "my-alb-123.us-east-1.elb.amazonaws.com")
        dns = _dns("Z1", "api.example.com", ["my-alb-123.us-east-1.elb.amazonaws.com"], zone_private=False)
        graph = Graph(nodes=[lb, dns])
        result = DnsToLoadBalancerStitcher().stitch(ORG_ID, graph, {})

        resolves_to = [e for e in result.edges if e.edge_type == EdgeType.RESOLVES_TO]
        assert len(resolves_to) == 1
        assert str(resolves_to[0].from_urn) == str(dns.urn)
        assert str(resolves_to[0].to_urn) == str(lb.urn)

    def test_dns_to_lb_case_insensitive(self):
        lb = _alb("my-alb", "MY-ALB.us-east-1.elb.amazonaws.com")
        dns = _dns("Z1", "app.example.com", ["my-alb.us-east-1.elb.amazonaws.com."])
        graph = Graph(nodes=[lb, dns])
        result = DnsToLoadBalancerStitcher().stitch(ORG_ID, graph, {})
        assert len([e for e in result.edges if e.edge_type == EdgeType.RESOLVES_TO]) == 1

    def test_no_match_returns_no_edges(self):
        lb = _alb("my-alb", "my-alb.elb.amazonaws.com")
        dns = _dns("Z1", "unrelated.example.com", ["1.2.3.4"])
        graph = Graph(nodes=[lb, dns])
        result = DnsToLoadBalancerStitcher().stitch(ORG_ID, graph, {})
        assert len(result.edges) == 0

    def test_dns_to_apigw_via_custom_domain(self):
        apigw = _apigw(
            "abc123", "https://abc123.execute-api.us-east-1.amazonaws.com",
            endpoint_type="HTTP",
        )
        apigw.metadata[NK.API_GW_CUSTOM_DOMAINS] = [
            "d-xyz789.execute-api.us-east-1.amazonaws.com",
            "api.example.com",
        ]
        dns = _dns("Z1", "api.example.com", ["d-xyz789.execute-api.us-east-1.amazonaws.com"])
        graph = Graph(nodes=[apigw, dns])
        result = DnsToLoadBalancerStitcher().stitch(ORG_ID, graph, {})
        resolves_to = [e for e in result.edges if e.edge_type == EdgeType.RESOLVES_TO]
        assert len(resolves_to) == 1
        assert str(resolves_to[0].to_urn) == str(apigw.urn)

    def test_dns_to_apigw_strips_https_prefix(self):
        apigw = _apigw("abc123", "https://abc123.execute-api.us-east-1.amazonaws.com")
        dns = _dns("Z1", "api.example.com", ["abc123.execute-api.us-east-1.amazonaws.com"])
        graph = Graph(nodes=[apigw, dns])
        result = DnsToLoadBalancerStitcher().stitch(ORG_ID, graph, {})
        assert len([e for e in result.edges if e.edge_type == EdgeType.RESOLVES_TO]) == 1

    def test_empty_graph(self):
        graph = Graph()
        result = DnsToLoadBalancerStitcher().stitch(ORG_ID, graph, {})
        assert len(result.edges) == 0
