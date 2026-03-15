"""Tests for VpcResourcePlugin."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from src.graph.graph_models import URN, NodeMetadataKey
from src.graph.loaders.aws.plugins.vpc_plugin import VpcResourcePlugin

ORG_ID = uuid.uuid4()
NK = NodeMetadataKey
ACCOUNT_URN = URN("urn:aws:account:123456789012:us-east-1:root")


def _make_session(vpcs, sgs, nacls):
    session = MagicMock()
    ec2 = MagicMock()
    session.client.return_value = ec2

    ec2.describe_vpcs.return_value = {"Vpcs": vpcs}

    # SG paginator
    sg_paginator = MagicMock()
    sg_paginator.paginate.return_value = [{"SecurityGroups": sgs}]

    # NACL paginator
    nacl_paginator = MagicMock()
    nacl_paginator.paginate.return_value = [{"NetworkAcls": nacls}]

    def get_paginator(name):
        if name == "describe_security_groups":
            return sg_paginator
        if name == "describe_network_acls":
            return nacl_paginator
        return MagicMock()

    ec2.get_paginator = get_paginator

    return session


class TestVpcResourcePlugin:
    def test_service_name(self):
        assert VpcResourcePlugin().service_name() == "vpc"

    def test_discover_vpc_sg_nacl(self):
        vpcs = [{"VpcId": "vpc-123", "CidrBlock": "10.0.0.0/16"}]
        sgs = [{
            "GroupId": "sg-abc",
            "GroupName": "web-sg",
            "VpcId": "vpc-123",
            "IpPermissions": [
                {
                    "IpProtocol": "tcp",
                    "FromPort": 443,
                    "ToPort": 443,
                    "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                    "Ipv6Ranges": [],
                    "UserIdGroupPairs": [],
                },
            ],
            "IpPermissionsEgress": [],
        }]
        nacls = [{
            "NetworkAclId": "acl-xyz",
            "VpcId": "vpc-123",
            "Entries": [
                {"RuleNumber": 100, "Protocol": "6", "RuleAction": "allow", "Egress": False, "CidrBlock": "0.0.0.0/0"},
            ],
        }]

        session = _make_session(vpcs, sgs, nacls)
        plugin = VpcResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        # VPC + SG + NACL
        node_types = {n.node_type for n in nodes}
        assert "vpc" in node_types
        assert "security_group" in node_types
        assert "nacl" in node_types

        # Contains edges: VPC->SG, VPC->NACL
        contains_edges = [e for e in edges if e.edge_type == "contains"]
        assert len(contains_edges) == 2

    def test_sg_to_sg_traffic_edge(self):
        vpcs = [{"VpcId": "vpc-123", "CidrBlock": "10.0.0.0/16"}]
        sgs = [{
            "GroupId": "sg-db",
            "GroupName": "db-sg",
            "VpcId": "vpc-123",
            "IpPermissions": [
                {
                    "IpProtocol": "tcp",
                    "FromPort": 5432,
                    "ToPort": 5432,
                    "IpRanges": [],
                    "Ipv6Ranges": [],
                    "UserIdGroupPairs": [{"GroupId": "sg-app"}],
                },
            ],
            "IpPermissionsEgress": [],
        }]

        session = _make_session(vpcs, sgs, [])
        plugin = VpcResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        traffic_edges = [e for e in edges if e.edge_type == "allows_traffic_to"]
        assert len(traffic_edges) == 1
        assert "sg-app" in str(traffic_edges[0].from_urn)
        assert "sg-db" in str(traffic_edges[0].to_urn)

    def test_discover_empty(self):
        session = _make_session([], [], [])
        plugin = VpcResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 0
        assert len(edges) == 0
