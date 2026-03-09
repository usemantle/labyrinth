"""
Tests for S3 wildcard collapsing and S3BucketLoader.

All AWS calls are mocked — no credentials or buckets required.
"""

import uuid
from unittest.mock import MagicMock

import pytest

from src.graph.graph_models import NodeMetadataKey
from src.graph.loaders.object_store.s3.loader import S3BucketLoader
from src.graph.loaders.object_store.s3.wildcard import (
    collapse_key,
    collapse_segment,
)

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
BUCKET_ARN = "arn:aws:s3:::my-test-bucket"
ACCOUNT_ID = "123456789012"
REGION = "us-east-1"


# ── Wildcard segment tests (unchanged — flat API still works) ─────


class TestCollapseSegment:
    def test_uuid(self):
        assert collapse_segment("550e8400-e29b-41d4-a716-446655440000") == "{uuid}"

    def test_uuid_uppercase(self):
        assert collapse_segment("550E8400-E29B-41D4-A716-446655440000") == "{uuid}"

    def test_iso_date(self):
        assert collapse_segment("2024-01-15") == "{date}"

    def test_hive_partition(self):
        assert collapse_segment("year=2024") == "year={*}"

    def test_hive_partition_with_underscore(self):
        assert collapse_segment("event_type=click") == "event_type={*}"

    def test_numeric_id(self):
        assert collapse_segment("12345") == "{id}"

    def test_numeric_single_digit_passthrough(self):
        assert collapse_segment("7") == "7"

    def test_iso_timestamp(self):
        assert collapse_segment("20240115T103000Z") == "{timestamp}"

    def test_iso_timestamp_no_z(self):
        assert collapse_segment("20240115T103000") == "{timestamp}"

    def test_hex_hash(self):
        assert collapse_segment("a3f2b8c901d4e5f6a7b8") == "{hash}"

    def test_hex_hash_short_passthrough(self):
        assert collapse_segment("a3f2b8c901d4e5f") == "a3f2b8c901d4e5f"

    def test_normal_segment_passthrough(self):
        assert collapse_segment("uploads") == "uploads"
        assert collapse_segment("images") == "images"
        assert collapse_segment("data.csv") == "data.csv"


class TestCollapseKey:
    def test_uuid_in_path(self):
        key = "uploads/550e8400-e29b-41d4-a716-446655440000/photo.jpg"
        assert collapse_key(key) == "uploads/{uuid}/photo.jpg"

    def test_date_and_id(self):
        key = "logs/2024-01-15/12345/output.csv"
        assert collapse_key(key) == "logs/{date}/{id}/output.csv"

    def test_hive_partitions(self):
        key = "data/year=2024/month=01/file.parquet"
        assert collapse_key(key) == "data/year={*}/month={*}/file.parquet"

    def test_no_variable_segments(self):
        key = "static/images/logo.png"
        assert collapse_key(key) == "static/images/logo.png"

    def test_single_segment(self):
        key = "readme.txt"
        assert collapse_key(key) == "readme.txt"


# ── S3BucketLoader tests (updated for hierarchical trie output) ───


def _mock_s3_client(keys: list[str]):
    """Build a mock S3 client that returns the given keys."""
    client = MagicMock()
    paginator = MagicMock()
    page = {"Contents": [{"Key": k} for k in keys]}
    paginator.paginate.return_value = [page]
    client.get_paginator.return_value = paginator
    return client


def _make_loader(keys: list[str], **kwargs) -> tuple[S3BucketLoader, list, list]:
    client = _mock_s3_client(keys)
    loader = S3BucketLoader(
        organization_id=ORG_ID,
        account_id=ACCOUNT_ID,
        region=REGION,
        s3_client=client,
        **kwargs,
    )
    nodes, edges = loader.load(BUCKET_ARN)
    return loader, nodes, edges


def _find_node(nodes, **metadata_match):
    """Find the first node whose metadata contains all key=value pairs."""
    for n in nodes:
        if all(n.metadata.get(k) == v for k, v in metadata_match.items()):
            return n
    return None


class TestS3BucketLoaderSingleFile:
    """data/report.csv → bucket, data (prefix), report.csv (leaf)."""

    @pytest.fixture(autouse=True)
    def setup(self):
        _, self.nodes, self.edges = _make_loader(["data/report.csv"])

    def test_node_count(self):
        # bucket + data (prefix) + report.csv (leaf)
        assert len(self.nodes) == 3

    def test_bucket_node(self):
        bucket = self.nodes[0]
        assert bucket.parent_urn is None
        assert bucket.metadata[NodeMetadataKey.BUCKET_NAME] == "my-test-bucket"
        assert bucket.metadata[NodeMetadataKey.ARN] == BUCKET_ARN

    def test_prefix_node(self):
        prefix = _find_node(
            self.nodes, **{NodeMetadataKey.PATH_PATTERN: "data"},
        )
        assert prefix is not None
        assert NodeMetadataKey.OBJECT_COUNT not in prefix.metadata

    def test_leaf_node(self):
        leaf = _find_node(
            self.nodes, **{NodeMetadataKey.PATH_PATTERN: "data/report.csv"},
        )
        assert leaf is not None
        assert leaf.metadata[NodeMetadataKey.OBJECT_COUNT] == 1

    def test_edge_count(self):
        # bucket→data, data→report.csv
        assert len(self.edges) == 2

    def test_parent_chain(self):
        bucket = self.nodes[0]
        prefix = _find_node(
            self.nodes, **{NodeMetadataKey.PATH_PATTERN: "data"},
        )
        leaf = _find_node(
            self.nodes, **{NodeMetadataKey.PATH_PATTERN: "data/report.csv"},
        )
        assert prefix.parent_urn == bucket.urn
        assert leaf.parent_urn == prefix.urn


class TestS3BucketLoaderCollapsing:
    """UUID and date collapsing with hierarchical output."""

    @pytest.fixture(autouse=True)
    def setup(self):
        keys = [
            "uploads/550e8400-e29b-41d4-a716-446655440000/photo.jpg",
            "uploads/661f9511-f30c-52e5-b827-557766551111/photo.jpg",
            "uploads/772a0622-a41d-63f6-c938-668877662222/photo.jpg",
            "logs/2024-01-15/events.json",
            "logs/2024-02-20/events.json",
        ]
        _, self.nodes, self.edges = _make_loader(keys)

    def test_hierarchy_structure(self):
        """Expected: bucket, uploads, {uuid}, photo.jpg, logs, {date}, events.json."""
        patterns = {
            n.metadata[NodeMetadataKey.PATH_PATTERN]
            for n in self.nodes
            if NodeMetadataKey.PATH_PATTERN in n.metadata
        }
        assert "uploads" in patterns
        assert "uploads/{uuid}" in patterns
        assert "uploads/{uuid}/photo.jpg" in patterns
        assert "logs" in patterns
        assert "logs/{date}" in patterns
        assert "logs/{date}/events.json" in patterns

    def test_uuid_partition_type(self):
        uuid_node = _find_node(
            self.nodes, **{NodeMetadataKey.PATH_PATTERN: "uploads/{uuid}"},
        )
        assert uuid_node.metadata[NodeMetadataKey.PARTITION_TYPE] == "{uuid}"

    def test_date_partition_type(self):
        date_node = _find_node(
            self.nodes, **{NodeMetadataKey.PATH_PATTERN: "logs/{date}"},
        )
        assert date_node.metadata[NodeMetadataKey.PARTITION_TYPE] == "{date}"

    def test_all_edges_are_contains(self):
        for edge in self.edges:
            assert edge.edge_type == "contains"


class TestS3BucketLoaderEmpty:
    def test_empty_bucket(self):
        client = _mock_s3_client([])
        loader = S3BucketLoader(
            organization_id=ORG_ID,
            account_id=ACCOUNT_ID,
            region=REGION,
            s3_client=client,
        )
        nodes, edges = loader.load(BUCKET_ARN)
        assert len(nodes) == 1
        assert len(edges) == 0
        assert nodes[0].metadata[NodeMetadataKey.BUCKET_NAME] == "my-test-bucket"


class TestS3BucketLoaderMaxKeys:
    def test_max_keys_passed_to_paginator(self):
        client = _mock_s3_client(["file.txt"])
        loader = S3BucketLoader(
            organization_id=ORG_ID,
            account_id=ACCOUNT_ID,
            region=REGION,
            s3_client=client,
            max_keys=100,
        )
        loader.load(BUCKET_ARN)
        paginator = client.get_paginator.return_value
        paginator.paginate.assert_called_once_with(
            Bucket="my-test-bucket",
            PaginationConfig={"MaxItems": 100},
        )


class TestS3BucketLoaderURN:
    def test_bucket_urn_structure(self):
        _, nodes, _ = _make_loader(["file.txt"])
        bucket = nodes[0]
        assert str(bucket.urn) == f"urn:aws:s3:{ACCOUNT_ID}:{REGION}:my-test-bucket"

    def test_leaf_urn_structure(self):
        _, nodes, _ = _make_loader(["data/file.txt"])
        leaf = _find_node(
            nodes, **{NodeMetadataKey.PATH_PATTERN: "data/file.txt"},
        )
        assert str(leaf.urn) == (
            f"urn:aws:s3:{ACCOUNT_ID}:{REGION}:my-test-bucket/data/file.txt"
        )

    def test_parent_urn_chain(self):
        _, nodes, _ = _make_loader(["data/file.txt"])
        leaf = _find_node(
            nodes, **{NodeMetadataKey.PATH_PATTERN: "data/file.txt"},
        )
        prefix = _find_node(
            nodes, **{NodeMetadataKey.PATH_PATTERN: "data"},
        )
        bucket = nodes[0]
        assert leaf.parent_urn == prefix.urn
        assert prefix.parent_urn == bucket.urn


class TestS3BucketLoaderOrgIsolation:
    def test_all_nodes_have_org_id(self):
        _, nodes, _ = _make_loader(["a.txt", "b.txt"])
        for node in nodes:
            assert node.organization_id == ORG_ID

    def test_all_edges_have_org_id(self):
        _, _, edges = _make_loader(["a.txt", "b.txt"])
        for edge in edges:
            assert edge.organization_id == ORG_ID


class TestS3BucketLoaderDeterministic:
    def test_edge_uuids_deterministic(self):
        """Running the loader twice produces identical edge UUIDs."""
        _, _, edges1 = _make_loader(["x/y.txt"])
        _, _, edges2 = _make_loader(["x/y.txt"])
        assert {e.uuid for e in edges1} == {e.uuid for e in edges2}


class TestS3ARNParsing:
    def test_valid_arn(self):
        name = S3BucketLoader._parse_bucket_name("arn:aws:s3:::my-bucket")
        assert name == "my-bucket"

    def test_arn_with_path(self):
        name = S3BucketLoader._parse_bucket_name("arn:aws:s3:::my-bucket/some/prefix")
        assert name == "my-bucket"

    def test_invalid_arn(self):
        with pytest.raises(ValueError, match="Invalid S3 ARN"):
            S3BucketLoader._parse_bucket_name("not-an-arn")


# ── Time partition integration tests ──────────────────────────────


class TestS3BucketLoaderHiveTimePartition:
    """Hive time partitions should collapse into a single node."""

    @pytest.fixture(autouse=True)
    def setup(self):
        keys = [
            "logs/org1/year=2025/month=12/day=29/hour=16/data.gz",
            "logs/org1/year=2026/month=01/day=03/hour=02/data.gz",
        ]
        _, self.nodes, self.edges = _make_loader(keys)

    def test_hive_time_partition_node_exists(self):
        htp = _find_node(
            self.nodes,
            **{NodeMetadataKey.PARTITION_TYPE: "{hive_time_partition}"},
        )
        assert htp is not None

    def test_hierarchy(self):
        patterns = {
            n.metadata[NodeMetadataKey.PATH_PATTERN]
            for n in self.nodes
            if NodeMetadataKey.PATH_PATTERN in n.metadata
        }
        assert "logs" in patterns
        assert "logs/org1" in patterns
        assert "logs/org1/{hive_time_partition}" in patterns
