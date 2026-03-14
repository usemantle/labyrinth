"""Tests for code-to-image stitching."""

import uuid

from src.graph.graph_models import (
    URN,
    EdgeMetadataKey,
    Node,
    NodeMetadata,
    NodeMetadataKey,
)
from src.graph.nodes.codebase_node import CodebaseNode
from src.graph.nodes.file_node import FileNode
from src.graph.nodes.image_node import ImageNode
from src.graph.nodes.image_repository_node import ImageRepositoryNode
from src.graph.stitching import stitch_code_to_images

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey
EK = EdgeMetadataKey


def _make_codebase_with_dockerfile(
    repo_name="my-app",
    repo_url="https://github.com/myorg/my-app",
):
    codebase_urn = URN(f"urn:github:repo:myorg:::{repo_name}")
    file_urn = URN(f"urn:github:repo:myorg:::{repo_name}/Dockerfile")

    codebase = CodebaseNode.create(ORG_ID, codebase_urn, repo_name=repo_name)
    codebase.metadata[NK.REPO_URL] = repo_url

    dockerfile = FileNode.create(
        ORG_ID, file_urn, codebase_urn,
        file_path="Dockerfile", language="unknown",
    )
    dockerfile.metadata[NK.DOCKERFILE_BASE_IMAGES] = "python:3.12-slim"

    return [codebase, dockerfile]


def _make_ecr_repo(
    repo_name="my-app",
    oci_source=None,
    oci_revision=None,
):
    repo_urn = URN(f"urn:aws:ecr:123:us-east-1:{repo_name}")
    repo_node = ImageRepositoryNode.create(
        ORG_ID, repo_urn,
        repository_name=repo_name,
        account_id="123",
        region="us-east-1",
    )

    image_urn = URN(f"urn:aws:ecr:123:us-east-1:{repo_name}/sha256:abc")
    image_node = ImageNode.create(
        ORG_ID, image_urn, repo_urn,
        image_digest="sha256:abc",
        image_tags="latest",
        oci_source=oci_source,
        oci_revision=oci_revision,
    )

    return [repo_node, image_node]


class TestStitchCodeToImagesOciLabel:
    def test_oci_source_match(self):
        code_nodes = _make_codebase_with_dockerfile(
            repo_url="https://github.com/myorg/my-app",
        )
        ecr_nodes = _make_ecr_repo(
            repo_name="my-app",
            oci_source="https://github.com/myorg/my-app",
        )
        all_nodes = code_nodes + ecr_nodes
        all_edges = []

        _, edges = stitch_code_to_images(ORG_ID, all_nodes, all_edges)
        builds = [e for e in edges if e.edge_type == "builds"]
        assert len(builds) == 1
        assert builds[0].metadata[EK.DETECTION_METHOD] == "oci_label"
        assert builds[0].metadata[EK.CONFIDENCE] == 1.0

    def test_oci_source_with_git_suffix(self):
        code_nodes = _make_codebase_with_dockerfile(
            repo_url="https://github.com/myorg/my-app",
        )
        ecr_nodes = _make_ecr_repo(
            repo_name="my-app",
            oci_source="https://github.com/myorg/my-app.git",
        )
        all_nodes = code_nodes + ecr_nodes

        _, edges = stitch_code_to_images(ORG_ID, all_nodes, [])
        builds = [e for e in edges if e.edge_type == "builds"]
        assert len(builds) == 1


class TestStitchCodeToImagesNameHeuristic:
    def test_name_match(self):
        code_nodes = _make_codebase_with_dockerfile(
            repo_name="my-app",
            repo_url="",  # No URL for OCI matching
        )
        # Clear repo_url so OCI matching can't fire
        code_nodes[0].metadata[NK.REPO_URL] = ""
        ecr_nodes = _make_ecr_repo(repo_name="my-app")
        all_nodes = code_nodes + ecr_nodes

        _, edges = stitch_code_to_images(ORG_ID, all_nodes, [])
        builds = [e for e in edges if e.edge_type == "builds"]
        assert len(builds) == 1
        assert builds[0].metadata[EK.DETECTION_METHOD] == "name_heuristic"
        assert builds[0].metadata[EK.CONFIDENCE] == 0.8

    def test_name_with_ecr_prefix(self):
        """ECR repos often have org/name format."""
        code_nodes = _make_codebase_with_dockerfile(repo_name="my-app", repo_url="")
        code_nodes[0].metadata[NK.REPO_URL] = ""
        ecr_nodes = _make_ecr_repo(repo_name="myorg/my-app")
        all_nodes = code_nodes + ecr_nodes

        _, edges = stitch_code_to_images(ORG_ID, all_nodes, [])
        builds = [e for e in edges if e.edge_type == "builds"]
        assert len(builds) == 1


class TestStitchCodeToImagesNoMatch:
    def test_no_dockerfiles(self):
        codebase_urn = URN("urn:github:repo:myorg:::my-app")
        codebase = CodebaseNode.create(ORG_ID, codebase_urn, repo_name="my-app")
        ecr_nodes = _make_ecr_repo(repo_name="my-app")
        all_nodes = [codebase] + ecr_nodes

        _, edges = stitch_code_to_images(ORG_ID, all_nodes, [])
        builds = [e for e in edges if e.edge_type == "builds"]
        assert len(builds) == 0

    def test_no_image_repos(self):
        code_nodes = _make_codebase_with_dockerfile()
        _, edges = stitch_code_to_images(ORG_ID, code_nodes, [])
        builds = [e for e in edges if e.edge_type == "builds"]
        assert len(builds) == 0

    def test_unrelated_names(self):
        code_nodes = _make_codebase_with_dockerfile(repo_name="my-app", repo_url="")
        code_nodes[0].metadata[NK.REPO_URL] = ""
        ecr_nodes = _make_ecr_repo(repo_name="totally-different")
        all_nodes = code_nodes + ecr_nodes

        _, edges = stitch_code_to_images(ORG_ID, all_nodes, [])
        builds = [e for e in edges if e.edge_type == "builds"]
        assert len(builds) == 0


class TestStitchOciTakesPriority:
    def test_oci_prevents_name_duplicate(self):
        """When OCI matches, name heuristic should not create a duplicate."""
        code_nodes = _make_codebase_with_dockerfile(
            repo_name="my-app",
            repo_url="https://github.com/myorg/my-app",
        )
        ecr_nodes = _make_ecr_repo(
            repo_name="my-app",
            oci_source="https://github.com/myorg/my-app",
        )
        all_nodes = code_nodes + ecr_nodes

        _, edges = stitch_code_to_images(ORG_ID, all_nodes, [])
        builds = [e for e in edges if e.edge_type == "builds"]
        # Should only get 1 edge (OCI), not 2 (OCI + name)
        assert len(builds) == 1
        assert builds[0].metadata[EK.DETECTION_METHOD] == "oci_label"
