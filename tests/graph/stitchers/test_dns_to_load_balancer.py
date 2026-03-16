"""Tests for DnsToLoadBalancerStitcher."""

import uuid

from src.graph.graph_models import URN, EdgeType, Graph, NodeMetadataKey
from src.graph.nodes.dns_record_node import DnsRecordNode
from src.graph.nodes.load_balancer_node import LoadBalancerNode
from src.graph.stitchers.dns_to_load_balancer import DnsToLoadBalancerStitcher

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey


class TestDnsToLoadBalancer:
    def test_dns_to_lb_alias_match(self):
        lb = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:elb:123:us-east-1:my-alb"),
            lb_type="alb", lb_scheme="internet-facing",
            lb_dns_name="my-alb-123.us-east-1.elb.amazonaws.com",
        )
        dns = DnsRecordNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:route53:123::Z1/api.example.com/A"),
            record_name="api.example.com", record_type="A", zone_private=False,
            values=["my-alb-123.us-east-1.elb.amazonaws.com"],
        )
        graph = Graph(nodes=[lb, dns])
        result = DnsToLoadBalancerStitcher().stitch(ORG_ID, graph, {})

        resolves_to = [e for e in result.edges if e.edge_type == EdgeType.RESOLVES_TO]
        assert len(resolves_to) == 1
        assert str(resolves_to[0].from_urn) == str(dns.urn)
        assert str(resolves_to[0].to_urn) == str(lb.urn)

    def test_dns_to_lb_case_insensitive(self):
        lb = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:elb:123:us-east-1:my-alb"),
            lb_type="alb", lb_scheme="internet-facing",
            lb_dns_name="MY-ALB.us-east-1.elb.amazonaws.com",
        )
        dns = DnsRecordNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:route53:123::Z1/app.example.com/A"),
            record_name="app.example.com", record_type="A",
            values=["my-alb.us-east-1.elb.amazonaws.com."],
        )
        graph = Graph(nodes=[lb, dns])
        result = DnsToLoadBalancerStitcher().stitch(ORG_ID, graph, {})
        assert len([e for e in result.edges if e.edge_type == EdgeType.RESOLVES_TO]) == 1

    def test_no_match_returns_no_edges(self):
        lb = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:elb:123:us-east-1:my-alb"),
            lb_type="alb", lb_scheme="internet-facing",
            lb_dns_name="my-alb.elb.amazonaws.com",
        )
        dns = DnsRecordNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:route53:123::Z1/unrelated.example.com/A"),
            record_name="unrelated.example.com", record_type="A",
            values=["1.2.3.4"],
        )
        graph = Graph(nodes=[lb, dns])
        result = DnsToLoadBalancerStitcher().stitch(ORG_ID, graph, {})
        assert len(result.edges) == 0

    def test_dns_to_apigw_via_custom_domain(self):
        apigw = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:apigateway:123:us-east-1:abc123"),
            lb_type="api_gateway_http", lb_scheme="internet-facing",
            lb_dns_name="https://abc123.execute-api.us-east-1.amazonaws.com",
            api_gw_endpoint_type="HTTP",
        )
        apigw.metadata[NK.API_GW_CUSTOM_DOMAINS] = [
            "d-xyz789.execute-api.us-east-1.amazonaws.com",
            "api.example.com",
        ]
        dns = DnsRecordNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:route53:123::Z1/api.example.com/A"),
            record_name="api.example.com", record_type="A",
            values=["d-xyz789.execute-api.us-east-1.amazonaws.com"],
        )
        graph = Graph(nodes=[apigw, dns])
        result = DnsToLoadBalancerStitcher().stitch(ORG_ID, graph, {})
        resolves_to = [e for e in result.edges if e.edge_type == EdgeType.RESOLVES_TO]
        assert len(resolves_to) == 1
        assert str(resolves_to[0].to_urn) == str(apigw.urn)

    def test_dns_to_apigw_strips_https_prefix(self):
        apigw = LoadBalancerNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:apigateway:123:us-east-1:abc123"),
            lb_type="api_gateway_http", lb_scheme="internet-facing",
            lb_dns_name="https://abc123.execute-api.us-east-1.amazonaws.com",
        )
        dns = DnsRecordNode.create(
            organization_id=ORG_ID,
            urn=URN("urn:aws:route53:123::Z1/api.example.com/A"),
            record_name="api.example.com", record_type="A",
            values=["abc123.execute-api.us-east-1.amazonaws.com"],
        )
        graph = Graph(nodes=[apigw, dns])
        result = DnsToLoadBalancerStitcher().stitch(ORG_ID, graph, {})
        assert len([e for e in result.edges if e.edge_type == EdgeType.RESOLVES_TO]) == 1

    def test_empty_graph(self):
        graph = Graph()
        result = DnsToLoadBalancerStitcher().stitch(ORG_ID, graph, {})
        assert len(result.edges) == 0
