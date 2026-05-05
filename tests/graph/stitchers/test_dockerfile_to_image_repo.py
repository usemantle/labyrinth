"""Tests for DockerfileToImageRepoStitcher."""

import uuid

from labyrinth.graph.graph_models import (
    URN,
    EdgeMetadataKey,
    Graph,
    NodeMetadataKey,
)
from labyrinth.graph.nodes.codebase_node import CodebaseNode
from labyrinth.graph.nodes.file_node import FileNode
from labyrinth.graph.nodes.image_node import ImageNode
from labyrinth.graph.nodes.image_repository_node import ImageRepositoryNode
from labyrinth.graph.stitchers.dockerfile_to_image_repo import DockerfileToImageRepoStitcher

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey
EK = EdgeMetadataKey


def _make_codebase_with_dockerfile(repo_name="my-app", repo_url="https://github.com/myorg/my-app"):
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


def _make_ecr_repo(repo_name="my-app", oci_source=None, oci_revision=None):
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


class TestOciLabel:
    def test_oci_source_match(self):
        code_nodes = _make_codebase_with_dockerfile(repo_url="https://github.com/myorg/my-app")
        ecr_nodes = _make_ecr_repo(repo_name="my-app", oci_source="https://github.com/myorg/my-app")
        graph = Graph(nodes=code_nodes + ecr_nodes)

        result = DockerfileToImageRepoStitcher().stitch(ORG_ID, graph, {})
        builds = [e for e in result.edges if e.edge_type == "builds"]
        assert len(builds) == 1
        assert builds[0].metadata[EK.DETECTION_METHOD] == "oci_label"
        assert builds[0].metadata[EK.CONFIDENCE] == 1.0

    def test_oci_source_with_git_suffix(self):
        code_nodes = _make_codebase_with_dockerfile(repo_url="https://github.com/myorg/my-app")
        ecr_nodes = _make_ecr_repo(repo_name="my-app", oci_source="https://github.com/myorg/my-app.git")
        graph = Graph(nodes=code_nodes + ecr_nodes)

        result = DockerfileToImageRepoStitcher().stitch(ORG_ID, graph, {})
        builds = [e for e in result.edges if e.edge_type == "builds"]
        assert len(builds) == 1


class TestNameHeuristic:
    def test_name_match(self):
        code_nodes = _make_codebase_with_dockerfile(repo_name="my-app", repo_url="")
        code_nodes[0].metadata[NK.REPO_URL] = ""
        ecr_nodes = _make_ecr_repo(repo_name="my-app")
        graph = Graph(nodes=code_nodes + ecr_nodes)

        result = DockerfileToImageRepoStitcher().stitch(ORG_ID, graph, {})
        builds = [e for e in result.edges if e.edge_type == "builds"]
        assert len(builds) == 1
        assert builds[0].metadata[EK.DETECTION_METHOD] == "name_heuristic"
        assert builds[0].metadata[EK.CONFIDENCE] == 0.8

    def test_name_with_ecr_prefix(self):
        code_nodes = _make_codebase_with_dockerfile(repo_name="my-app", repo_url="")
        code_nodes[0].metadata[NK.REPO_URL] = ""
        ecr_nodes = _make_ecr_repo(repo_name="myorg/my-app")
        graph = Graph(nodes=code_nodes + ecr_nodes)

        result = DockerfileToImageRepoStitcher().stitch(ORG_ID, graph, {})
        builds = [e for e in result.edges if e.edge_type == "builds"]
        assert len(builds) == 1


class TestNoMatch:
    def test_no_dockerfiles(self):
        codebase_urn = URN("urn:github:repo:myorg:::my-app")
        codebase = CodebaseNode.create(ORG_ID, codebase_urn, repo_name="my-app")
        ecr_nodes = _make_ecr_repo(repo_name="my-app")
        graph = Graph(nodes=[codebase] + ecr_nodes)

        result = DockerfileToImageRepoStitcher().stitch(ORG_ID, graph, {})
        builds = [e for e in result.edges if e.edge_type == "builds"]
        assert len(builds) == 0

    def test_no_image_repos(self):
        code_nodes = _make_codebase_with_dockerfile()
        graph = Graph(nodes=code_nodes)

        result = DockerfileToImageRepoStitcher().stitch(ORG_ID, graph, {})
        builds = [e for e in result.edges if e.edge_type == "builds"]
        assert len(builds) == 0

    def test_unrelated_names(self):
        code_nodes = _make_codebase_with_dockerfile(repo_name="my-app", repo_url="")
        code_nodes[0].metadata[NK.REPO_URL] = ""
        ecr_nodes = _make_ecr_repo(repo_name="totally-different")
        graph = Graph(nodes=code_nodes + ecr_nodes)

        result = DockerfileToImageRepoStitcher().stitch(ORG_ID, graph, {})
        builds = [e for e in result.edges if e.edge_type == "builds"]
        assert len(builds) == 0


class TestOciTakesPriority:
    def test_oci_prevents_name_duplicate(self):
        code_nodes = _make_codebase_with_dockerfile(
            repo_name="my-app", repo_url="https://github.com/myorg/my-app",
        )
        ecr_nodes = _make_ecr_repo(repo_name="my-app", oci_source="https://github.com/myorg/my-app")
        graph = Graph(nodes=code_nodes + ecr_nodes)

        result = DockerfileToImageRepoStitcher().stitch(ORG_ID, graph, {})
        builds = [e for e in result.edges if e.edge_type == "builds"]
        assert len(builds) == 1
        assert builds[0].metadata[EK.DETECTION_METHOD] == "oci_label"
