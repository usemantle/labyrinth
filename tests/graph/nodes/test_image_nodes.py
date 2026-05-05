"""Tests for ImageRepositoryNode and ImageNode."""

import uuid

from labyrinth.graph.graph_models import URN, Node, NodeMetadataKey
from labyrinth.graph.nodes.image_node import ImageNode
from labyrinth.graph.nodes.image_repository_node import ImageRepositoryNode

NK = NodeMetadataKey
ORG_ID = uuid.uuid4()


class TestImageRepositoryNode:
    def test_is_subclass_of_node(self):
        assert issubclass(ImageRepositoryNode, Node)

    def test_node_type(self):
        node = ImageRepositoryNode(
            organization_id=ORG_ID,
            urn=URN("urn:aws:ecr:123:us-east-1:my-repo"),
        )
        assert node.node_type == "image_repository"

    def test_create(self):
        urn = URN("urn:aws:ecr:123:us-east-1:my-repo")
        node = ImageRepositoryNode.create(
            ORG_ID, urn,
            repository_name="my-repo",
            repository_uri="123.dkr.ecr.us-east-1.amazonaws.com/my-repo",
            arn="arn:aws:ecr:us-east-1:123:repository/my-repo",
            account_id="123",
            region="us-east-1",
        )
        assert node.metadata[NK.REPOSITORY_NAME] == "my-repo"
        assert node.metadata[NK.REPOSITORY_URI] == "123.dkr.ecr.us-east-1.amazonaws.com/my-repo"
        assert node.metadata[NK.ARN] == "arn:aws:ecr:us-east-1:123:repository/my-repo"
        assert node.metadata[NK.ACCOUNT_ID] == "123"
        assert node.metadata[NK.REGION] == "us-east-1"

    def test_create_minimal(self):
        urn = URN("urn:aws:ecr:123:us-east-1:my-repo")
        node = ImageRepositoryNode.create(ORG_ID, urn, repository_name="my-repo")
        assert node.metadata[NK.REPOSITORY_NAME] == "my-repo"
        assert NK.ARN not in node.metadata


class TestImageNode:
    def test_is_subclass_of_node(self):
        assert issubclass(ImageNode, Node)

    def test_node_type(self):
        node = ImageNode(
            organization_id=ORG_ID,
            urn=URN("urn:aws:ecr:123:us-east-1:my-repo/sha256:abc"),
        )
        assert node.node_type == "image"

    def test_create(self):
        urn = URN("urn:aws:ecr:123:us-east-1:my-repo/sha256:abc")
        parent = URN("urn:aws:ecr:123:us-east-1:my-repo")
        node = ImageNode.create(
            ORG_ID, urn, parent,
            image_digest="sha256:abc",
            image_tags="latest,v1.0",
            image_pushed_at="2024-01-01T00:00:00Z",
            image_size_bytes=1024,
            oci_source="https://github.com/org/repo",
            oci_revision="abc123",
        )
        assert node.metadata[NK.IMAGE_DIGEST] == "sha256:abc"
        assert node.metadata[NK.IMAGE_TAGS] == "latest,v1.0"
        assert node.metadata[NK.OCI_SOURCE] == "https://github.com/org/repo"
        assert node.metadata[NK.OCI_REVISION] == "abc123"

    def test_create_minimal(self):
        urn = URN("urn:aws:ecr:123:us-east-1:my-repo/sha256:abc")
        node = ImageNode.create(ORG_ID, urn, image_digest="sha256:abc")
        assert node.metadata[NK.IMAGE_DIGEST] == "sha256:abc"
        assert NK.OCI_SOURCE not in node.metadata
