"""Tests for S3ResourcePlugin with deep path discovery."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from labyrinth.graph.graph_models import URN, NodeMetadataKey
from labyrinth.graph.loaders.aws.plugins.s3_plugin import S3ResourcePlugin

ORG_ID = uuid.uuid4()
NK = NodeMetadataKey
ACCOUNT_ID = "123456789012"
REGION = "us-east-1"
ACCOUNT_URN = URN(f"urn:aws:account:{ACCOUNT_ID}:{REGION}:root")


def _build_urn(*segments: str) -> URN:
    return URN("urn:test:test:test:test:" + "/".join(segments))


def _make_session(buckets, keys_by_bucket=None, bucket_region="us-east-1"):
    """Build a mock session.

    Args:
        buckets: list of bucket names
        keys_by_bucket: dict mapping bucket name -> list of object keys
        bucket_region: region to return for all buckets
    """
    session = MagicMock()
    s3 = MagicMock()
    session.client.return_value = s3
    s3.list_buckets.return_value = {
        "Buckets": [{"Name": b} for b in buckets],
    }
    s3.get_bucket_location.return_value = {
        "LocationConstraint": bucket_region if bucket_region != "us-east-1" else None,
    }

    if keys_by_bucket is None:
        keys_by_bucket = {}

    # Mock paginator for list_objects_v2
    def make_paginator(op):
        if op != "list_objects_v2":
            raise ValueError(f"Unexpected paginator: {op}")
        paginator = MagicMock()

        def paginate(**kwargs):
            bucket_name = kwargs["Bucket"]
            keys = keys_by_bucket.get(bucket_name, [])
            if keys:
                return [{"Contents": [{"Key": k} for k in keys]}]
            return [{"Contents": []}]

        paginator.paginate.side_effect = paginate
        return paginator

    s3.get_paginator.side_effect = make_paginator
    return session


class TestS3ResourcePlugin:
    def test_service_name(self):
        assert S3ResourcePlugin().service_name() == "s3"

    def test_discover_buckets(self):
        session = _make_session(["my-bucket", "other-bucket"])
        plugin = S3ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id=ACCOUNT_ID, region=REGION,
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=_build_urn,
        )

        bucket_nodes = [n for n in nodes if n.node_type == "s3_bucket"]
        assert len(bucket_nodes) == 2
        names = {n.metadata[NK.BUCKET_NAME] for n in bucket_nodes}
        assert names == {"my-bucket", "other-bucket"}

    def test_discover_filters_by_region(self):
        session = _make_session(["my-bucket"], bucket_region="eu-west-1")
        plugin = S3ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id=ACCOUNT_ID, region=REGION,
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=_build_urn,
        )

        assert len(nodes) == 0

    def test_discover_empty(self):
        session = _make_session([])
        plugin = S3ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id=ACCOUNT_ID, region=REGION,
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=_build_urn,
        )

        assert len(nodes) == 0
        assert len(edges) == 0