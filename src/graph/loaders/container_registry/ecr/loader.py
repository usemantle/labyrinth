"""AWS ECR repository loader for the security graph.

URN scheme: urn:aws:ecr:{account_id}:{region}:{repository_name}

Discovers a single ECR repository's images, extracts OCI standard
labels from image configs, and emits a hierarchical set of nodes.
"""

from __future__ import annotations

import json
import logging
import uuid

from src.graph.credentials import AWSProfileCredential, CredentialBase
from src.graph.edges.contains_edge import ContainsEdge
from src.graph.graph_models import (
    URN,
    Edge,
    Node,
)
from src.graph.loaders.loader import ConceptLoader, URNComponent
from src.graph.nodes.image_node import ImageNode
from src.graph.nodes.image_repository_node import ImageRepositoryNode

logger = logging.getLogger(__name__)

# OCI standard label keys
_OCI_SOURCE = "org.opencontainers.image.source"
_OCI_REVISION = "org.opencontainers.image.revision"

# Media types we accept when pulling manifests
_ACCEPTED_MANIFEST_TYPES = [
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
]


class EcrLoader(ConceptLoader):
    """Loader for a single AWS ECR repository.

    Discovers the repository, its tagged images, and extracts OCI
    labels from image configs for downstream stitching.

    Args:
        organization_id: Tenant UUID.
        account_id: AWS account ID for URN construction.
        region: AWS region for URN construction.
        ecr_client: A boto3 ECR client (or mock).
    """

    def __init__(
        self,
        organization_id: uuid.UUID,
        *,
        account_id: str = "_",
        region: str = "_",
        ecr_client=None,
    ):
        super().__init__(organization_id)
        self._account_id = account_id
        self._region = region
        self._ecr_client = ecr_client

    def build_urn(self, *path_segments: str) -> URN:
        path = "/".join(path_segments)
        return URN(
            f"urn:aws:ecr:{self._account_id}:{self._region}:{path}"
        )

    @classmethod
    def display_name(cls) -> str:
        return "AWS ECR Repository"

    @classmethod
    def urn_components(cls) -> list[URNComponent]:
        return [
            URNComponent("account_id", "AWS account ID"),
            URNComponent("region", "AWS region"),
            URNComponent("repository", "ECR repository name"),
        ]

    @classmethod
    def credential_type(cls) -> type[CredentialBase]:
        return AWSProfileCredential

    @classmethod
    def build_target_urn(cls, **components: str) -> URN:
        return URN(
            f"urn:aws:ecr:{components['account_id']}:{components['region']}:{components['repository']}"
        )

    @classmethod
    def from_target_config(
        cls, project_id: uuid.UUID, urn: URN, credentials: dict, **kwargs,
    ) -> tuple[EcrLoader, str]:
        import boto3

        session = boto3.Session(profile_name=credentials.get("profile", "default"))
        ecr_client = session.client("ecr", region_name=urn.region)
        loader = cls(
            project_id,
            account_id=urn.account,
            region=urn.region,
            ecr_client=ecr_client,
        )
        # Resource string is the repository name (last URN segment)
        return loader, urn.path

    def load(self, resource: str) -> tuple[list[Node], list[Edge]]:
        """Load nodes and edges from an ECR repository.

        Args:
            resource: ECR repository name.
        """
        repo_name = resource
        nodes: list[Node] = []
        edges: list[Edge] = []

        # 1. Describe the repository
        repo_meta = self._describe_repository(repo_name)
        if repo_meta is None:
            logger.warning("Repository %s not found", repo_name)
            return nodes, edges

        repo_urn = self.build_urn(repo_name)
        repo_node = ImageRepositoryNode.create(
            organization_id=self.organization_id,
            urn=repo_urn,
            repository_name=repo_name,
            repository_uri=repo_meta.get("repositoryUri"),
            arn=repo_meta.get("repositoryArn"),
            account_id=self._account_id,
            region=self._region,
        )
        nodes.append(repo_node)

        # 2. List tagged images
        images = self._list_tagged_images(repo_name)
        if not images:
            logger.info("Repository %s has no tagged images", repo_name)
            return nodes, edges

        # 3. Extract OCI labels from image configs
        oci_labels = self._extract_oci_labels(repo_name, images)

        # 4. Create ImageNode per unique digest
        for img in images:
            digest = img["imageDigest"]
            tags = ",".join(sorted(img.get("imageTags", [])))
            image_urn = self.build_urn(repo_name, digest)

            labels = oci_labels.get(digest, {})
            image_node = ImageNode.create(
                organization_id=self.organization_id,
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
                self.organization_id, repo_urn, image_urn,
            ))

        logger.info(
            "Discovered %d images in ECR repository %s",
            len(images), repo_name,
        )
        return nodes, edges

    # ------------------------------------------------------------------
    # ECR API helpers
    # ------------------------------------------------------------------

    def _describe_repository(self, repo_name: str) -> dict | None:
        try:
            resp = self._ecr_client.describe_repositories(
                repositoryNames=[repo_name],
            )
            repos = resp.get("repositories", [])
            return repos[0] if repos else None
        except self._ecr_client.exceptions.RepositoryNotFoundException:
            return None

    def _list_tagged_images(self, repo_name: str) -> list[dict]:
        images: list[dict] = []
        paginator = self._ecr_client.get_paginator("describe_images")
        for page in paginator.paginate(
            repositoryName=repo_name,
            filter={"tagStatus": "TAGGED"},
        ):
            images.extend(page.get("imageDetails", []))
        return images

    def _extract_oci_labels(
        self, repo_name: str, images: list[dict],
    ) -> dict[str, dict[str, str]]:
        """Extract OCI labels from image manifests/configs.

        Returns a dict of digest -> {label_key: label_value}.
        """
        labels_by_digest: dict[str, dict[str, str]] = {}

        # Build image IDs for batch_get_image
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
            resp = self._ecr_client.batch_get_image(
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

                # Fetch the config blob
                config_resp = self._ecr_client.batch_get_image(
                    repositoryName=repo_name,
                    imageIds=[{"imageDigest": config_digest}],
                    acceptedMediaTypes=[
                        config.get("mediaType", "application/vnd.oci.image.config.v1+json"),
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
