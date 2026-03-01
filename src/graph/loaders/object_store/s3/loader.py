"""AWS S3 bucket loader for the security graph.

URN scheme: urn:aws:s3:{account_id}:{region}:{bucket}/{collapsed_path}

Lists all objects in a bucket, builds a trie of path segments,
collapses variable segments (UUIDs, dates, hive time partitions, etc.)
using pluggable matchers, and emits a hierarchical set of nodes.
"""

import logging
import uuid

from src.graph.graph_models import (
    Edge,
    Node,
    NodeMetadata,
    NodeMetadataKey,
    RelationType,
    URN,
)
from src.graph.loaders._helpers import make_edge
from src.graph.loaders.loader import ConceptLoader
from src.graph.loaders.object_store.s3.matchers import (
    SegmentMatcher,
    SequenceMatcher,
)
from src.graph.loaders.object_store.s3.trie import TrieNode
from src.graph.loaders.object_store.s3.wildcard import build_collapsed_trie

logger = logging.getLogger(__name__)

_MAX_SAMPLES = 3


class S3BucketLoader(ConceptLoader):
    """Loader for AWS S3 buckets.

    Discovers object keys, collapses variable segments into a
    hierarchical trie, and connects nodes via CONTAINS edges.

    Args:
        organization_id: Tenant UUID.
        account_id: AWS account ID for URN construction.
        region: AWS region for URN construction.
        s3_client: A boto3 S3 client (or mock).
        max_keys: Optional cap on number of keys to list.
        sequence_matchers: Custom multi-segment matchers (or None for defaults).
        segment_matchers: Custom single-segment matchers (or None for defaults).
    """

    def __init__(
        self,
        organization_id: uuid.UUID,
        *,
        account_id: str = "_",
        region: str = "_",
        s3_client=None,
        max_keys: int | None = None,
        sequence_matchers: list[SequenceMatcher] | None = None,
        segment_matchers: list[SegmentMatcher] | None = None,
    ):
        super().__init__(organization_id)
        self._account_id = account_id
        self._region = region
        self._s3_client = s3_client
        self._max_keys = max_keys
        self._sequence_matchers = sequence_matchers
        self._segment_matchers = segment_matchers

    def build_urn(self, *path_segments: str) -> URN:
        path = "/".join(path_segments)
        return URN(
            f"urn:aws:s3:{self._account_id}:{self._region}:{path}"
        )

    def load(self, resource: str) -> tuple[list[Node], list[Edge]]:
        """Load nodes and edges from an S3 bucket.

        Args:
            resource: An S3 bucket ARN (e.g. ``arn:aws:s3:::my-bucket``).
        """
        bucket_name = self._parse_bucket_name(resource)

        nodes: list[Node] = []
        edges: list[Edge] = []

        bucket_urn = self.build_urn(bucket_name)
        nodes.append(self._build_bucket_node(bucket_urn, bucket_name, resource))

        keys = self._list_object_keys(bucket_name)

        if not keys:
            logger.info("Bucket %s is empty", bucket_name)
            return nodes, edges

        trie_nodes, trie_edges = self._build_trie_nodes(
            keys, bucket_urn, bucket_name,
        )
        nodes.extend(trie_nodes)
        edges.extend(trie_edges)

        logger.info(
            "Discovered %d hierarchy nodes from %d keys in %s",
            len(trie_nodes), len(keys), bucket_name,
        )

        return nodes, edges

    def _build_bucket_node(
        self, bucket_urn: URN, bucket_name: str, arn: str,
    ) -> Node:
        return Node(
            organization_id=self.organization_id,
            urn=bucket_urn,
            parent_urn=None,
            metadata=NodeMetadata({
                NodeMetadataKey.BUCKET_NAME: bucket_name,
                NodeMetadataKey.ARN: arn,
                NodeMetadataKey.ACCOUNT_ID: self._account_id,
                NodeMetadataKey.REGION: self._region,
            }),
        )

    def _list_object_keys(self, bucket_name: str) -> list[str]:
        keys: list[str] = []
        paginator = self._s3_client.get_paginator("list_objects_v2")
        page_kwargs: dict = {"Bucket": bucket_name}
        if self._max_keys is not None:
            page_kwargs["PaginationConfig"] = {"MaxItems": self._max_keys}

        for page in paginator.paginate(**page_kwargs):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

        return keys

    def _build_trie_nodes(
        self,
        keys: list[str],
        bucket_urn: URN,
        bucket_name: str,
    ) -> tuple[list[Node], list[Edge]]:
        """Build hierarchical nodes from collapsed trie."""
        trie = build_collapsed_trie(
            keys,
            sequence_matchers=self._sequence_matchers,
            segment_matchers=self._segment_matchers,
            max_samples=_MAX_SAMPLES,
        )

        nodes: list[Node] = []
        edges: list[Edge] = []

        for path_segments, trie_node in trie.walk():
            path_str = "/".join(path_segments)
            node_urn = self.build_urn(bucket_name, path_str)

            if len(path_segments) > 1:
                parent_path = "/".join(path_segments[:-1])
                parent_urn = self.build_urn(bucket_name, parent_path)
            else:
                parent_urn = bucket_urn

            nodes.append(Node(
                organization_id=self.organization_id,
                urn=node_urn,
                parent_urn=parent_urn,
                metadata=self._build_node_metadata(path_str, trie_node),
            ))
            edges.append(make_edge(
                self.organization_id,
                parent_urn,
                node_urn,
                RelationType.CONTAINS,
            ))

        return nodes, edges

    @staticmethod
    def _build_node_metadata(
        path_str: str, trie_node: TrieNode,
    ) -> NodeMetadata:
        meta = NodeMetadata({
            NodeMetadataKey.PATH_PATTERN: path_str,
        })
        if trie_node.is_leaf:
            meta[NodeMetadataKey.OBJECT_COUNT] = trie_node.key_count
        if trie_node.sample_keys:
            meta[NodeMetadataKey.SAMPLE_KEYS] = ",".join(
                trie_node.sample_keys[:_MAX_SAMPLES],
            )
        if trie_node.collapsed_token:
            meta[NodeMetadataKey.PARTITION_TYPE] = trie_node.collapsed_token
        return meta

    @staticmethod
    def _parse_bucket_name(arn: str) -> str:
        # arn:aws:s3:::{bucket_name}
        parts = arn.split(":")
        if len(parts) < 6 or parts[2] != "s3":
            raise ValueError(f"Invalid S3 ARN: {arn}")
        return parts[5].split("/")[0]
