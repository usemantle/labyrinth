"""S3 resource discovery plugin for AwsAccountLoader."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable

import boto3

from src.graph.edges.contains_edge import ContainsEdge
from src.graph.graph_models import URN, Edge, Node, NodeMetadataKey
from src.graph.loaders.aws.plugins._base import AwsResourcePlugin
from src.graph.loaders.object_store.s3.wildcard import build_collapsed_trie
from src.graph.nodes.bucket_node import BucketNode
from src.graph.nodes.object_path_node import ObjectPathNode

logger = logging.getLogger(__name__)

NK = NodeMetadataKey

_MAX_SAMPLES = 3


class S3ResourcePlugin(AwsResourcePlugin):
    """Discover S3 buckets and their object path hierarchy in the account."""

    def service_name(self) -> str:
        return "s3"

    def discover(
        self,
        session: boto3.Session,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        build_urn: Callable[..., URN],
    ) -> tuple[list[Node], list[Edge]]:
        s3 = session.client("s3")
        nodes: list[Node] = []
        edges: list[Edge] = []

        try:
            response = s3.list_buckets()
        except Exception:
            logger.exception("Failed to list S3 buckets")
            return nodes, edges

        for bucket in response.get("Buckets", []):
            bucket_name = bucket["Name"]

            # Filter to buckets in our target region
            try:
                loc = s3.get_bucket_location(Bucket=bucket_name)
                bucket_region = loc.get("LocationConstraint") or "us-east-1"
            except Exception:
                logger.debug("Cannot determine region for bucket %s, skipping", bucket_name)
                continue

            if bucket_region != region:
                continue

            bucket_urn = URN(f"urn:aws:s3:{account_id}:{region}:{bucket_name}")
            arn = f"arn:aws:s3:::{bucket_name}"

            node = BucketNode.create(
                organization_id=organization_id,
                urn=bucket_urn,
                parent_urn=account_urn,
                bucket_name=bucket_name,
                arn=arn,
                account_id=account_id,
                region=region,
            )
            nodes.append(node)

            # Discover object path hierarchy
            path_nodes, path_edges = self._discover_paths(
                s3, bucket_name, bucket_urn,
                organization_id, account_id, region,
            )
            nodes.extend(path_nodes)
            edges.extend(path_edges)

        return nodes, edges

    def _discover_paths(
        self,
        s3,
        bucket_name: str,
        bucket_urn: URN,
        organization_id: uuid.UUID,
        account_id: str,
        region: str,
    ) -> tuple[list[Node], list[Edge]]:
        """List objects and build collapsed path hierarchy for a bucket."""
        nodes: list[Node] = []
        edges: list[Edge] = []

        keys = self._list_object_keys(s3, bucket_name)
        if not keys:
            return nodes, edges

        trie = build_collapsed_trie(keys, max_samples=_MAX_SAMPLES)

        for path_segments, trie_node in trie.walk():
            path_str = "/".join(path_segments)
            node_urn = URN(
                f"urn:aws:s3:{account_id}:{region}:{bucket_name}/{path_str}"
            )

            if len(path_segments) > 1:
                parent_path = "/".join(path_segments[:-1])
                parent_urn = URN(
                    f"urn:aws:s3:{account_id}:{region}:{bucket_name}/{parent_path}"
                )
            else:
                parent_urn = bucket_urn

            obj_node = ObjectPathNode.create(
                organization_id=organization_id,
                urn=node_urn,
                parent_urn=parent_urn,
                path_pattern=path_str,
                object_count=trie_node.key_count if trie_node.is_leaf else None,
                sample_keys=(
                    ",".join(trie_node.sample_keys[:_MAX_SAMPLES])
                    if trie_node.sample_keys
                    else None
                ),
            )
            if trie_node.collapsed_token:
                obj_node.metadata[NK.PARTITION_TYPE] = trie_node.collapsed_token
            nodes.append(obj_node)
            edges.append(ContainsEdge.create(
                organization_id, parent_urn, node_urn,
            ))

        logger.info(
            "Discovered %d hierarchy nodes from %d keys in %s",
            len(nodes), len(keys), bucket_name,
        )
        return nodes, edges

    @staticmethod
    def _list_object_keys(s3, bucket_name: str) -> list[str]:
        """List all object keys in a bucket."""
        keys: list[str] = []
        try:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
        except Exception:
            logger.warning("Failed to list objects in bucket %s", bucket_name)
        return keys
