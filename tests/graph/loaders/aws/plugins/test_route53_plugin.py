"""Tests for Route53ResourcePlugin."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from labyrinth.graph.graph_models import URN, NodeMetadataKey
from labyrinth.graph.loaders.aws.plugins.route53_plugin import Route53ResourcePlugin

ORG_ID = uuid.uuid4()
NK = NodeMetadataKey
ACCOUNT_URN = URN("urn:aws:account:123456789012:us-east-1:root")


def _make_session(zones, records_by_zone):
    session = MagicMock()
    route53 = MagicMock()
    session.client.return_value = route53

    zone_paginator = MagicMock()
    zone_paginator.paginate.return_value = [{"HostedZones": zones}]

    def get_paginator(name):
        if name == "list_hosted_zones":
            return zone_paginator
        if name == "list_resource_record_sets":
            record_paginator = MagicMock()

            def paginate(HostedZoneId):
                records = records_by_zone.get(HostedZoneId, [])
                return [{"ResourceRecordSets": records}]

            record_paginator.paginate = paginate
            return record_paginator
        return MagicMock()

    route53.get_paginator = get_paginator

    return session


class TestRoute53ResourcePlugin:
    def test_service_name(self):
        assert Route53ResourcePlugin().service_name() == "route53"

    def test_discover_a_and_cname_records(self):
        zones = [
            {
                "Id": "/hostedzone/Z123",
                "Name": "example.com.",
                "Config": {"PrivateZone": False},
            },
        ]
        records = [
            {
                "Name": "api.example.com.",
                "Type": "A",
                "TTL": 300,
                "ResourceRecords": [{"Value": "1.2.3.4"}],
            },
            {
                "Name": "www.example.com.",
                "Type": "CNAME",
                "TTL": 600,
                "ResourceRecords": [{"Value": "api.example.com"}],
            },
            {
                "Name": "example.com.",
                "Type": "SOA",
                "TTL": 900,
                "ResourceRecords": [{"Value": "ns-1.example.com"}],
            },
            {
                "Name": "example.com.",
                "Type": "NS",
                "TTL": 300,
                "ResourceRecords": [{"Value": "ns-1.example.com"}],
            },
        ]
        records_by_zone = {"/hostedzone/Z123": records}

        session = _make_session(zones, records_by_zone)
        plugin = Route53ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        # Should have 2 records (SOA and NS skipped)
        assert len(nodes) == 2
        node_types = {n.node_type for n in nodes}
        assert "dns_record" in node_types

        # Check metadata on the A record
        a_nodes = [n for n in nodes if n.metadata.get(NK.DNS_RECORD_TYPE) == "A"]
        assert len(a_nodes) == 1
        assert a_nodes[0].metadata[NK.DNS_RECORD_NAME] == "api.example.com"
        assert a_nodes[0].metadata[NK.DNS_ZONE_PRIVATE] is False
        assert a_nodes[0].metadata[NK.DNS_VALUES] == ["1.2.3.4"]

    def test_discover_alias_record(self):
        zones = [
            {
                "Id": "/hostedzone/Z456",
                "Name": "example.com.",
                "Config": {"PrivateZone": False},
            },
        ]
        records = [
            {
                "Name": "app.example.com.",
                "Type": "A",
                "AliasTarget": {
                    "DNSName": "my-alb-123.us-east-1.elb.amazonaws.com.",
                    "HostedZoneId": "Z35SXDOTRQ7X7K",
                    "EvaluateTargetHealth": True,
                },
            },
        ]
        records_by_zone = {"/hostedzone/Z456": records}

        session = _make_session(zones, records_by_zone)
        plugin = Route53ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 1
        assert nodes[0].metadata[NK.DNS_VALUES] == ["my-alb-123.us-east-1.elb.amazonaws.com"]

    def test_discover_no_zones(self):
        session = _make_session([], {})
        plugin = Route53ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 0
        assert len(edges) == 0
