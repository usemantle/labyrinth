"""Tests for EcrResourcePlugin."""

from __future__ import annotations

import json
import uuid
from unittest.mock import MagicMock

from src.graph.graph_models import URN, NodeMetadataKey
from src.graph.loaders.aws.plugins.ecr_plugin import EcrResourcePlugin

ORG_ID = uuid.uuid4()
NK = NodeMetadataKey
ACCOUNT_ID = "123456789012"
REGION = "us-east-1"
ACCOUNT_URN = URN(f"urn:aws:account:{ACCOUNT_ID}:{REGION}:root")


def _build_urn(*segments: str) -> URN:
    return URN("urn:test:test:test:test:" + "/".join(segments))


def _make_session(repos, images=None, manifests=None):
    session = MagicMock()
    ecr = MagicMock()
    session.client.return_value = ecr

    # describe_repositories paginator
    repo_paginator = MagicMock()
    repo_paginator.paginate.return_value = [{"repositories": repos}]

    # describe_images paginator
    img_paginator = MagicMock()
    img_paginator.paginate.return_value = [
        {"imageDetails": images or []}
    ]

    def get_paginator(op):
        if op == "describe_repositories":
            return repo_paginator
        if op == "describe_images":
            return img_paginator
        raise ValueError(f"Unexpected paginator: {op}")

    ecr.get_paginator.side_effect = get_paginator

    # batch_get_image
    if manifests is not None:
        ecr.batch_get_image.side_effect = manifests
    else:
        ecr.batch_get_image.return_value = {"images": []}

    return session


class TestEcrResourcePlugin:
    def test_service_name(self):
        assert EcrResourcePlugin().service_name() == "ecr"

    def test_discover_repos(self):
        repos = [
            {
                "repositoryName": "my-app",
                "repositoryUri": f"{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/my-app",
                "repositoryArn": f"arn:aws:ecr:{REGION}:{ACCOUNT_ID}:repository/my-app",
            },
        ]
        session = _make_session(repos)
        plugin = EcrResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id=ACCOUNT_ID, region=REGION,
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=_build_urn,
        )

        repo_nodes = [n for n in nodes if n.node_type == "image_repository"]
        assert len(repo_nodes) == 1
        assert repo_nodes[0].metadata[NK.REPOSITORY_NAME] == "my-app"
        assert repo_nodes[0].parent_urn == ACCOUNT_URN

    def test_discover_empty(self):
        session = _make_session([])
        plugin = EcrResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id=ACCOUNT_ID, region=REGION,
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=_build_urn,
        )

        assert len(nodes) == 0


class TestEcrResourcePluginImageDiscovery:
    def test_tagged_images_discovered(self):
        repos = [
            {
                "repositoryName": "my-app",
                "repositoryUri": f"{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/my-app",
                "repositoryArn": f"arn:aws:ecr:{REGION}:{ACCOUNT_ID}:repository/my-app",
            },
        ]
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
        session = _make_session(repos, images=images)
        plugin = EcrResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id=ACCOUNT_ID, region=REGION,
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=_build_urn,
        )

        repo_nodes = [n for n in nodes if n.node_type == "image_repository"]
        image_nodes = [n for n in nodes if n.node_type == "image"]
        assert len(repo_nodes) == 1
        assert len(image_nodes) == 2
        assert len(edges) == 2
        assert all(e.edge_type == "contains" for e in edges)

        digests = {n.metadata[NK.IMAGE_DIGEST] for n in image_nodes}
        assert digests == {"sha256:aaa111", "sha256:bbb222"}

        # Tags should be comma-separated and sorted
        for img_node in image_nodes:
            if img_node.metadata[NK.IMAGE_DIGEST] == "sha256:aaa111":
                assert img_node.metadata[NK.IMAGE_TAGS] == "latest,v1.0"

    def test_empty_repo_only_repo_node(self):
        repos = [
            {
                "repositoryName": "empty-repo",
                "repositoryUri": f"{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/empty-repo",
                "repositoryArn": f"arn:aws:ecr:{REGION}:{ACCOUNT_ID}:repository/empty-repo",
            },
        ]
        session = _make_session(repos, images=[])
        plugin = EcrResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id=ACCOUNT_ID, region=REGION,
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=_build_urn,
        )

        assert len(nodes) == 1
        assert nodes[0].node_type == "image_repository"
        assert len(edges) == 0


class TestEcrResourcePluginOciLabels:
    def test_oci_labels_extracted(self):
        repos = [
            {
                "repositoryName": "my-app",
                "repositoryUri": f"{ACCOUNT_ID}.dkr.ecr.{REGION}.amazonaws.com/my-app",
                "repositoryArn": f"arn:aws:ecr:{REGION}:{ACCOUNT_ID}:repository/my-app",
            },
        ]
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

        batch_responses = [
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

        session = _make_session(repos, images=images, manifests=batch_responses)
        plugin = EcrResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id=ACCOUNT_ID, region=REGION,
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=_build_urn,
        )

        image_nodes = [n for n in nodes if n.node_type == "image"]
        assert len(image_nodes) == 1
        assert image_nodes[0].metadata[NK.OCI_SOURCE] == "https://github.com/myorg/my-app"
        assert image_nodes[0].metadata[NK.OCI_REVISION] == "abc123def"
