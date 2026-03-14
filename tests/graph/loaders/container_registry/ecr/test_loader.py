"""Tests for EcrLoader.

All AWS calls are mocked — no credentials or ECR repos required.
"""

import json
import uuid
from unittest.mock import MagicMock

import pytest

from src.graph.graph_models import NodeMetadataKey
from src.graph.loaders.container_registry.ecr.loader import EcrLoader

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
ACCOUNT_ID = "123456789012"
REGION = "us-east-1"
REPO_NAME = "my-app"

NK = NodeMetadataKey


# ── Fixtures ──────────────────────────────────────────────────────────


def _build_mock_ecr_client(
    repo_exists=True,
    images=None,
    manifests=None,
):
    """Build a mock boto3 ECR client."""
    client = MagicMock()

    # Set up exceptions
    not_found = type("RepositoryNotFoundException", (Exception,), {})
    client.exceptions.RepositoryNotFoundException = not_found

    if repo_exists:
        client.describe_repositories.return_value = {
            "repositories": [{
                "repositoryName": REPO_NAME,
                "repositoryUri": f"{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/{REPO_NAME}",
                "repositoryArn": f"arn:aws:ecr:{REGION}:{ACCOUNT_ID}:repository/{REPO_NAME}",
            }]
        }
    else:
        client.describe_repositories.side_effect = not_found()

    # Mock paginator for describe_images
    paginator = MagicMock()
    client.get_paginator.return_value = paginator
    paginator.paginate.return_value = [
        {"imageDetails": images or []}
    ]

    # Mock batch_get_image
    client.batch_get_image.return_value = {
        "images": manifests or [],
    }

    return client


# ── Tests ─────────────────────────────────────────────────────────────


class TestEcrLoaderBasics:
    def test_display_name(self):
        assert EcrLoader.display_name() == "AWS ECR Repository"

    def test_urn_components(self):
        components = EcrLoader.urn_components()
        names = [c.name for c in components]
        assert "account_id" in names
        assert "region" in names
        assert "repository" in names

    def test_build_target_urn(self):
        urn = EcrLoader.build_target_urn(
            account_id=ACCOUNT_ID, region=REGION, repository=REPO_NAME,
        )
        assert str(urn) == f"urn:aws:ecr:{ACCOUNT_ID}:{REGION}:{REPO_NAME}"


class TestEcrLoaderEmptyRepo:
    def test_empty_repo(self):
        client = _build_mock_ecr_client(images=[])
        loader = EcrLoader(ORG_ID, account_id=ACCOUNT_ID, region=REGION, ecr_client=client)
        nodes, edges = loader.load(REPO_NAME)

        # Should have only the repository node
        assert len(nodes) == 1
        assert nodes[0].node_type == "image_repository"
        assert nodes[0].metadata[NK.REPOSITORY_NAME] == REPO_NAME
        assert len(edges) == 0


class TestEcrLoaderRepoNotFound:
    def test_repo_not_found(self):
        client = _build_mock_ecr_client(repo_exists=False)
        loader = EcrLoader(ORG_ID, account_id=ACCOUNT_ID, region=REGION, ecr_client=client)
        nodes, edges = loader.load(REPO_NAME)
        assert len(nodes) == 0
        assert len(edges) == 0


class TestEcrLoaderWithImages:
    def test_tagged_images(self):
        images = [
            {
                "imageDigest": "sha256:aaa111",
                "imageTags": ["latest", "v1.0"],
                "imagePushedAt": "2024-06-01T12:00:00Z",
                "imageSizeInBytes": 50_000_000,
            },
            {
                "imageDigest": "sha256:bbb222",
                "imageTags": ["v0.9"],
                "imagePushedAt": "2024-05-15T10:00:00Z",
                "imageSizeInBytes": 48_000_000,
            },
        ]

        client = _build_mock_ecr_client(images=images)
        loader = EcrLoader(ORG_ID, account_id=ACCOUNT_ID, region=REGION, ecr_client=client)
        nodes, edges = loader.load(REPO_NAME)

        # 1 repo + 2 images = 3 nodes
        assert len(nodes) == 3
        repo_nodes = [n for n in nodes if n.node_type == "image_repository"]
        image_nodes = [n for n in nodes if n.node_type == "image"]
        assert len(repo_nodes) == 1
        assert len(image_nodes) == 2

        # 2 contains edges
        assert len(edges) == 2
        assert all(e.edge_type == "contains" for e in edges)

        # Verify image metadata
        digests = {n.metadata[NK.IMAGE_DIGEST] for n in image_nodes}
        assert digests == {"sha256:aaa111", "sha256:bbb222"}

        # Verify tags are comma-separated and sorted
        for img_node in image_nodes:
            if img_node.metadata[NK.IMAGE_DIGEST] == "sha256:aaa111":
                assert img_node.metadata[NK.IMAGE_TAGS] == "latest,v1.0"


class TestEcrLoaderWithOciLabels:
    def test_oci_labels_extracted(self):
        images = [
            {
                "imageDigest": "sha256:aaa111",
                "imageTags": ["latest"],
                "imagePushedAt": "2024-06-01T12:00:00Z",
                "imageSizeInBytes": 50_000_000,
            },
        ]

        manifest = json.dumps({
            "config": {
                "digest": "sha256:config123",
                "mediaType": "application/vnd.oci.image.config.v1+json",
            },
        })

        config_blob = json.dumps({
            "config": {
                "Labels": {
                    "org.opencontainers.image.source": "https://github.com/myorg/my-app",
                    "org.opencontainers.image.revision": "abc123def",
                },
            },
        })

        client = _build_mock_ecr_client(images=images)

        # First call returns manifest, second returns config blob
        client.batch_get_image.side_effect = [
            {
                "images": [{
                    "imageId": {"imageDigest": "sha256:aaa111"},
                    "imageManifest": manifest,
                }],
            },
            {
                "images": [{
                    "imageId": {"imageDigest": "sha256:config123"},
                    "imageManifest": config_blob,
                }],
            },
        ]

        loader = EcrLoader(ORG_ID, account_id=ACCOUNT_ID, region=REGION, ecr_client=client)
        nodes, edges = loader.load(REPO_NAME)

        image_nodes = [n for n in nodes if n.node_type == "image"]
        assert len(image_nodes) == 1
        assert image_nodes[0].metadata[NK.OCI_SOURCE] == "https://github.com/myorg/my-app"
        assert image_nodes[0].metadata[NK.OCI_REVISION] == "abc123def"


class TestEcrLoaderUrnScheme:
    def test_urn_format(self):
        client = _build_mock_ecr_client(images=[])
        loader = EcrLoader(ORG_ID, account_id=ACCOUNT_ID, region=REGION, ecr_client=client)
        nodes, _ = loader.load(REPO_NAME)

        repo_urn = str(nodes[0].urn)
        assert repo_urn == f"urn:aws:ecr:{ACCOUNT_ID}:{REGION}:{REPO_NAME}"
