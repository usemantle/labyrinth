"""Tests for Elbv2ResourcePlugin."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from labyrinth.graph.graph_models import URN, NodeMetadataKey
from labyrinth.graph.loaders.aws.plugins.elbv2_plugin import Elbv2ResourcePlugin

ORG_ID = uuid.uuid4()
NK = NodeMetadataKey
ACCOUNT_URN = URN("urn:aws:account:123456789012:us-east-1:root")


def _make_session(load_balancers, target_groups, listeners):
    session = MagicMock()
    elbv2 = MagicMock()
    session.client.return_value = elbv2

    lb_paginator = MagicMock()
    lb_paginator.paginate.return_value = [{"LoadBalancers": load_balancers}]

    listener_paginator = MagicMock()
    listener_paginator.paginate.return_value = [{"Listeners": listeners}]

    tg_paginator = MagicMock()
    tg_paginator.paginate.return_value = [{"TargetGroups": target_groups}]

    def get_paginator(name):
        if name == "describe_load_balancers":
            return lb_paginator
        if name == "describe_listeners":
            return listener_paginator
        if name == "describe_target_groups":
            return tg_paginator
        return MagicMock()

    elbv2.get_paginator = get_paginator

    return session


class TestElbv2ResourcePlugin:
    def test_service_name(self):
        assert Elbv2ResourcePlugin().service_name() == "elbv2"

    def test_discover_alb_with_target_group(self):
        lb_arn = "arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/app/my-alb/abc123"
        tg_arn = "arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/my-tg/def456"

        load_balancers = [{
            "LoadBalancerName": "my-alb",
            "LoadBalancerArn": lb_arn,
            "Type": "application",
            "Scheme": "internet-facing",
            "DNSName": "my-alb-123.us-east-1.elb.amazonaws.com",
            "State": {"Code": "active"},
            "SecurityGroups": ["sg-12345"],
        }]

        target_groups = [{
            "TargetGroupName": "my-tg",
            "TargetGroupArn": tg_arn,
            "Port": 8080,
            "Protocol": "HTTP",
            "TargetType": "ip",
            "HealthCheckEnabled": True,
            "HealthCheckProtocol": "HTTP",
            "HealthCheckPath": "/health",
            "HealthCheckIntervalSeconds": 30,
        }]

        listeners = [{
            "Port": 443,
            "Protocol": "HTTPS",
            "DefaultActions": [{"Type": "forward", "TargetGroupArn": tg_arn}],
        }]

        session = _make_session(load_balancers, target_groups, listeners)
        plugin = Elbv2ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        # Should have LB + target group
        node_types = {n.node_type for n in nodes}
        assert "load_balancer" in node_types
        assert "backend_group" in node_types

        # Check LB metadata
        lb_nodes = [n for n in nodes if n.node_type == "load_balancer"]
        assert len(lb_nodes) == 1
        assert lb_nodes[0].metadata[NK.LB_TYPE] == "alb"
        assert lb_nodes[0].metadata[NK.LB_SCHEME] == "internet-facing"
        assert lb_nodes[0].metadata[NK.LB_DNS_NAME] == "my-alb-123.us-east-1.elb.amazonaws.com"

        # Check BG metadata
        bg_nodes = [n for n in nodes if n.node_type == "backend_group"]
        assert len(bg_nodes) == 1
        assert bg_nodes[0].metadata[NK.BG_NAME] == "my-tg"
        assert bg_nodes[0].metadata[NK.BG_PORT] == 8080
        assert bg_nodes[0].metadata[NK.BG_TARGET_TYPE] == "ip"

        # Check edges
        edge_types = {e.edge_type for e in edges}
        assert "protected_by" in edge_types  # LB -> SG
        assert "routes_to" in edge_types  # LB -> BG

    def test_discover_nlb(self):
        lb_arn = "arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/net/my-nlb/abc"
        load_balancers = [{
            "LoadBalancerName": "my-nlb",
            "LoadBalancerArn": lb_arn,
            "Type": "network",
            "Scheme": "internal",
            "DNSName": "my-nlb.internal.elb.amazonaws.com",
            "State": {"Code": "active"},
        }]

        session = _make_session(load_balancers, [], [])
        plugin = Elbv2ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 1
        assert nodes[0].metadata[NK.LB_TYPE] == "nlb"
        assert nodes[0].metadata[NK.LB_SCHEME] == "internal"

    def test_discover_no_lbs(self):
        session = _make_session([], [], [])
        plugin = Elbv2ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 0
        assert len(edges) == 0
