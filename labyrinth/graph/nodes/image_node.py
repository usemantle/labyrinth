"""ImageNode — a specific container image manifest identified by digest."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType
from labyrinth.graph.nodes._aws_resource_mixin import AwsResourceMixin

NK = NodeMetadataKey


@dataclass
class ImageNode(AwsResourceMixin, Node):
    """An AWS ECR container image identified by digest."""

    node_type: str = NodeType.IMAGE

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset()
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
    })

    @classmethod
    def build_urn(
        cls,
        account_id: str,
        region: str,
        repository_name: str,
        image_digest: str,
    ) -> URN:
        return cls.urn_from_arn(
            f"arn:aws:ecr:{region}:{account_id}:repository/{repository_name}/image/{image_digest}",
        )

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        image_digest: str,
        image_tags: str | None = None,
        image_pushed_at: str | None = None,
        image_size_bytes: int | None = None,
        oci_source: str | None = None,
        oci_revision: str | None = None,
    ) -> ImageNode:
        meta = NodeMetadata({NK.IMAGE_DIGEST: image_digest})
        if image_tags is not None:
            meta[NK.IMAGE_TAGS] = image_tags
        if image_pushed_at is not None:
            meta[NK.IMAGE_PUSHED_AT] = image_pushed_at
        if image_size_bytes is not None:
            meta[NK.IMAGE_SIZE_BYTES] = image_size_bytes
        if oci_source is not None:
            meta[NK.OCI_SOURCE] = oci_source
        if oci_revision is not None:
            meta[NK.OCI_REVISION] = oci_revision
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
