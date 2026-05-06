"""Tests for RdsResourcePlugin."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from labyrinth.graph.graph_models import URN, NodeMetadataKey
from labyrinth.graph.loaders.aws.plugins.rds_plugin import RdsResourcePlugin

ORG_ID = uuid.uuid4()
NK = NodeMetadataKey
ACCOUNT_URN = URN("urn:aws:account:123456789012:us-east-1:root")


def _make_session(instances):
    session = MagicMock()
    rds = MagicMock()
    session.client.return_value = rds

    paginator = MagicMock()
    rds.get_paginator.return_value = paginator
    paginator.paginate.return_value = [{"DBInstances": instances}]

    return session


class TestRdsResourcePlugin:
    def test_service_name(self):
        assert RdsResourcePlugin().service_name() == "rds"

    def test_discover_instance(self):
        instances = [{
            "DBInstanceIdentifier": "my-db",
            "Engine": "postgres",
            "Endpoint": {"Address": "my-db.abc.us-east-1.rds.amazonaws.com", "Port": 5432},
            "PubliclyAccessible": False,
            "StorageEncrypted": True,
            "MultiAZ": True,
            "DBInstanceArn": "arn:aws:rds:us-east-1:123:db:my-db",
            "VpcSecurityGroups": [
                {"VpcSecurityGroupId": "sg-abc", "Status": "active"},
            ],
            "DBSubnetGroup": {"VpcId": "vpc-123"},
        }]
        session = _make_session(instances)
        plugin = RdsResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 1
        assert nodes[0].node_type == "rds_instance"
        assert nodes[0].metadata[NK.RDS_ENGINE] == "postgres"
        assert nodes[0].metadata[NK.RDS_PUBLICLY_ACCESSIBLE] is False
        assert nodes[0].metadata[NK.RDS_ENCRYPTION_ENABLED] is True

        # Should have a ProtectedByEdge to the security group
        protected_edges = [e for e in edges if e.edge_type == "protected_by"]
        assert len(protected_edges) == 1
        assert "sg-abc" in str(protected_edges[0].to_urn)

    def test_discover_empty(self):
        session = _make_session([])
        plugin = RdsResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 0
        assert len(edges) == 0
