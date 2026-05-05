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


class TestS3ResourcePluginDeepPaths:
    def test_single_file_hierarchy(self):
        """data/report.csv → bucket + data (prefix) + report.csv (leaf)."""
        session = _make_session(
            ["my-bucket"],
            keys_by_bucket={"my-bucket": ["data/report.csv"]},
        )
        plugin = S3ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id=ACCOUNT_ID, region=REGION,
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=_build_urn,
        )

        bucket_nodes = [n for n in nodes if n.node_type == "s3_bucket"]
        path_nodes = [n for n in nodes if n.node_type == "s3_prefix"]
        assert len(bucket_nodes) == 1
        assert len(path_nodes) == 2  # "data" prefix + "data/report.csv" leaf
        assert len(edges) == 2

        patterns = {n.metadata[NK.PATH_PATTERN] for n in path_nodes}
        assert "data" in patterns
        assert "data/report.csv" in patterns

    def test_empty_bucket_no_paths(self):
        """Empty bucket should produce only a BucketNode, no paths."""
        session = _make_session(
            ["empty-bucket"],
            keys_by_bucket={"empty-bucket": []},
        )
        plugin = S3ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id=ACCOUNT_ID, region=REGION,
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=_build_urn,
        )

        assert len(nodes) == 1
        assert nodes[0].node_type == "s3_bucket"
        assert len(edges) == 0

    def test_variable_segment_collapsing(self):
        """UUIDs and dates in paths should collapse to wildcards."""
        keys = [
            "uploads/550e8400-e29b-41d4-a716-446655440000/photo.jpg",
            "uploads/661f9511-f30c-52e5-b827-557766551111/photo.jpg",
            "uploads/772a0622-a41d-63f6-c938-668877662222/photo.jpg",
            "logs/2024-01-15/events.json",
            "logs/2024-02-20/events.json",
        ]
        session = _make_session(
            ["my-bucket"],
            keys_by_bucket={"my-bucket": keys},
        )
        plugin = S3ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id=ACCOUNT_ID, region=REGION,
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=_build_urn,
        )

        path_nodes = [n for n in nodes if n.node_type == "s3_prefix"]
        patterns = {
            n.metadata[NK.PATH_PATTERN]
            for n in path_nodes
        }

        assert "uploads" in patterns
        assert "uploads/{uuid}" in patterns
        assert "uploads/{uuid}/photo.jpg" in patterns
        assert "logs" in patterns
        assert "logs/{date}" in patterns
        assert "logs/{date}/events.json" in patterns

    def test_partition_type_metadata(self):
        """Collapsed nodes should have PARTITION_TYPE metadata."""
        keys = [
            "uploads/550e8400-e29b-41d4-a716-446655440000/photo.jpg",
            "uploads/661f9511-f30c-52e5-b827-557766551111/photo.jpg",
        ]
        session = _make_session(
            ["my-bucket"],
            keys_by_bucket={"my-bucket": keys},
        )
        plugin = S3ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id=ACCOUNT_ID, region=REGION,
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=_build_urn,
        )

        uuid_nodes = [
            n for n in nodes
            if n.metadata.get(NK.PARTITION_TYPE) == "{uuid}"
        ]
        assert len(uuid_nodes) == 1

    def test_all_edges_are_contains(self):
        keys = ["data/file.txt"]
        session = _make_session(
            ["my-bucket"],
            keys_by_bucket={"my-bucket": keys},
        )
        plugin = S3ResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id=ACCOUNT_ID, region=REGION,
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=_build_urn,
        )

        for edge in edges:
            assert edge.edge_type == "contains"
