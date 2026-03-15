"""Tests for S3ResourcePlugin."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from src.graph.graph_models import URN, NodeMetadataKey
from src.graph.loaders.aws.plugins.s3_plugin import S3ResourcePlugin

ORG_ID = uuid.uuid4()
NK = NodeMetadataKey
ACCOUNT_URN = URN("urn:aws:account:123456789012:us-east-1:root")


def _make_session(buckets, bucket_region="us-east-1"):
    session = MagicMock()
    s3 = MagicMock()
    session.client.return_value = s3
    s3.list_buckets.return_value = {
        "Buckets": [{"Name": b} for b in buckets],
    }
    s3.get_bucket_location.return_value = {
        "LocationConstraint": bucket_region if bucket_region != "us-east-1" else None,
    }
    return session


class TestS3ResourcePlugin:
    def test_service_name(self):
        assert S3ResourcePlugin().service_name() == "s3"

    def test_discover_buckets(self):
        session = _make_session(["my-bucket", "other-bucket"])
        plugin = S3ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 2
        assert all(n.node_type == "s3_bucket" for n in nodes)
        names = {n.metadata[NK.BUCKET_NAME] for n in nodes}
        assert names == {"my-bucket", "other-bucket"}

    def test_discover_filters_by_region(self):
        session = _make_session(["my-bucket"], bucket_region="eu-west-1")
        plugin = S3ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 0

    def test_discover_empty(self):
        session = _make_session([])
        plugin = S3ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 0
        assert len(edges) == 0
