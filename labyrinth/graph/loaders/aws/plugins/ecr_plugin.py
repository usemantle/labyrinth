"""ECR resource discovery plugin for AwsAccountLoader."""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Callable

import boto3

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.graph_models import URN, Edge, Node
from labyrinth.graph.loaders.aws.plugins._base import AwsResourcePlugin
from labyrinth.graph.nodes.image_node import ImageNode
from labyrinth.graph.nodes.image_repository_node import ImageRepositoryNode

logger = logging.getLogger(__name__)

# OCI standard label keys
_OCI_SOURCE = "org.opencontainers.image.source"
_OCI_REVISION = "org.opencontainers.image.revision"

# Media types we accept when pulling manifests
_ACCEPTED_MANIFEST_TYPES = [
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
]


class EcrResourcePlugin(AwsResourcePlugin):
    """Discover ECR repositories and their images in the account."""

    def service_name(self) -> str:
        return "ecr"

    def discover(
        self,
        session: boto3.Session,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        build_urn: Callable[..., URN],
    ) -> tuple[list[Node], list[Edge]]:
        ecr = session.client("ecr", region_name=region)
        nodes: list[Node] = []
        edges: list[Edge] = []

        try:
            paginator = ecr.get_paginator("describe_repositories")
            for page in paginator.paginate():
                for repo in page.get("repositories", []):
                    repo_name = repo["repositoryName"]
                    repo_uri = repo.get("repositoryUri", "")
                    repo_arn = repo.get("repositoryArn", "")

                    ecr_urn = ImageRepositoryNode.build_urn(account_id, region, repo_name)

                    node = ImageRepositoryNode.create(
                        organization_id=organization_id,
                        urn=ecr_urn,
                        parent_urn=account_urn,
                        repository_name=repo_name,
                        repository_uri=repo_uri,
                        arn=repo_arn,
                        account_id=account_id,
                        region=region,
                    )
                    nodes.append(node)

                    # Discover tagged images in this repo
                    img_nodes, img_edges = self._discover_images(
                        ecr, repo_name, ecr_urn, organization_id,
                        account_id, region,
                    )
                    nodes.extend(img_nodes)
                    edges.extend(img_edges)
        except Exception:
            logger.exception("Failed to describe ECR repositories")

        return nodes, edges

    def _discover_images(
        self,
        ecr,
        repo_name: str,
        repo_urn: URN,
        organization_id: uuid.UUID,
        account_id: str,
        region: str,
    ) -> tuple[list[Node], list[Edge]]:
        """Discover tagged images and extract OCI labels for a repository."""
        nodes: list[Node] = []
        edges: list[Edge] = []

        # List tagged images
        images = self._list_tagged_images(ecr, repo_name)
        if not images:
            return nodes, edges

        # Extract OCI labels
        oci_labels = self._extract_oci_labels(ecr, repo_name, images)

        # Create ImageNode per unique digest
        for img in images:
            digest = img["imageDigest"]
            tags = ",".join(sorted(img.get("imageTags", [])))
            image_urn = ImageNode.build_urn(account_id, region, repo_name, digest)

            labels = oci_labels.get(digest, {})
            image_node = ImageNode.create(
                organization_id=organization_id,
                urn=image_urn,
                parent_urn=repo_urn,
                image_digest=digest,
                image_tags=tags or None,
                image_pushed_at=str(img.get("imagePushedAt", "")) or None,
                image_size_bytes=img.get("imageSizeInBytes"),
                oci_source=labels.get(_OCI_SOURCE),
                oci_revision=labels.get(_OCI_REVISION),
            )
            nodes.append(image_node)
            edges.append(ContainsEdge.create(
                organization_id, repo_urn, image_urn,
            ))

        logger.info(
            "Discovered %d images in ECR repository %s",
            len(images), repo_name,
        )
        return nodes, edges

    @staticmethod
    def _list_tagged_images(ecr, repo_name: str) -> list[dict]:
        """List all tagged images in a repository."""
        images: list[dict] = []
        try:
            paginator = ecr.get_paginator("describe_images")
            for page in paginator.paginate(
                repositoryName=repo_name,
                filter={"tagStatus": "TAGGED"},
            ):
                images.extend(page.get("imageDetails", []))
        except Exception:
            logger.warning("Failed to describe images for %s", repo_name)
        return images

    @staticmethod
    def _extract_oci_labels(
        ecr, repo_name: str, images: list[dict],
    ) -> dict[str, dict[str, str]]:
        """Extract OCI labels from image manifests/configs.

        Returns a dict of digest -> {label_key: label_value}.
        """
        labels_by_digest: dict[str, dict[str, str]] = {}

        image_ids = []
        for img in images:
            tags = img.get("imageTags", [])
            if tags:
                image_ids.append({
                    "imageDigest": img["imageDigest"],
                    "imageTag": tags[0],
                })

        if not image_ids:
            return labels_by_digest

        try:
            resp = ecr.batch_get_image(
                repositoryName=repo_name,
                imageIds=image_ids,
                acceptedMediaTypes=_ACCEPTED_MANIFEST_TYPES,
            )
        except Exception:
            logger.warning("Failed to batch_get_image for %s", repo_name)
            return labels_by_digest

        for image in resp.get("images", []):
            digest = image["imageId"]["imageDigest"]
            manifest_str = image.get("imageManifest", "")
            if not manifest_str:
                continue

            try:
                manifest = json.loads(manifest_str)
                config = manifest.get("config", {})
                config_digest = config.get("digest")
                if not config_digest:
                    continue

                config_resp = ecr.batch_get_image(
                    repositoryName=repo_name,
                    imageIds=[{"imageDigest": config_digest}],
                    acceptedMediaTypes=[
                        config.get(
                            "mediaType",
                            "application/vnd.oci.image.config.v1+json",
                        ),
                    ],
                )
                for config_image in config_resp.get("images", []):
                    config_manifest = config_image.get("imageManifest", "")
                    if config_manifest:
                        config_data = json.loads(config_manifest)
                        oci_config = config_data.get("config", {})
                        labels = oci_config.get("Labels", {})
                        if labels:
                            labels_by_digest[digest] = labels
            except (json.JSONDecodeError, KeyError):
                continue

        return labels_by_digest
