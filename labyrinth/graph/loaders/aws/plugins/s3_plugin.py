"""S3 resource discovery plugin for AwsAccountLoader."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable

import boto3

from labyrinth.graph.graph_models import URN, Edge, Node, NodeMetadataKey
from labyrinth.graph.loaders.aws.plugins._base import AwsResourcePlugin
from labyrinth.graph.nodes.bucket_node import BucketNode

logger = logging.getLogger(__name__)

NK = NodeMetadataKey


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
            try:
                # todo: enrich the node metadata with security relevant information
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

        return nodes, edges
