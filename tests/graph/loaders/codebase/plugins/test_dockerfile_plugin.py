"""Tests for the Dockerfile plugin."""

import uuid

from src.graph.graph_models import NodeMetadataKey
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from src.graph.loaders.codebase.plugins.dockerfile_plugin import DockerfilePlugin

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey


def _make_repo_with_dockerfile(tmp_path, dockerfile_content, filename="Dockerfile"):
    """Create a minimal repo with a Dockerfile."""
    repo = tmp_path / "my-app"
    repo.mkdir()
    (repo / filename).write_text(dockerfile_content)
    (repo / "app.py").write_text("def main():\n    pass\n")
    return repo


class TestDockerfilePluginDetection:
    def test_simple_dockerfile(self, tmp_path):
        repo = _make_repo_with_dockerfile(
            tmp_path,
            "FROM python:3.12-slim\nCOPY . .\nCMD [\"python\", \"app.py\"]\n",
        )
        loader = FileSystemCodebaseLoader(
            organization_id=ORG_ID,
            plugins=[DockerfilePlugin()],
        )
        nodes, edges = loader.load(str(repo))

        df_nodes = [
            n for n in nodes
            if n.node_type == "file" and NK.DOCKERFILE_BASE_IMAGES in n.metadata
        ]
        assert len(df_nodes) == 1
        assert df_nodes[0].metadata[NK.DOCKERFILE_BASE_IMAGES] == "python:3.12-slim"

    def test_multistage_dockerfile(self, tmp_path):
        repo = _make_repo_with_dockerfile(
            tmp_path,
            "FROM node:20 AS builder\nRUN npm build\n"
            "FROM nginx:alpine\nCOPY --from=builder /app/dist /usr/share/nginx/html\n",
        )
        loader = FileSystemCodebaseLoader(
            organization_id=ORG_ID,
            plugins=[DockerfilePlugin()],
        )
        nodes, edges = loader.load(str(repo))

        df_nodes = [
            n for n in nodes
            if n.node_type == "file" and NK.DOCKERFILE_BASE_IMAGES in n.metadata
        ]
        assert len(df_nodes) == 1
        base_images = df_nodes[0].metadata[NK.DOCKERFILE_BASE_IMAGES]
        assert "node:20" in base_images
        assert "nginx:alpine" in base_images

    def test_no_dockerfile(self, tmp_path):
        repo = tmp_path / "my-app"
        repo.mkdir()
        (repo / "app.py").write_text("def main():\n    pass\n")

        loader = FileSystemCodebaseLoader(
            organization_id=ORG_ID,
            plugins=[DockerfilePlugin()],
        )
        nodes, edges = loader.load(str(repo))

        df_nodes = [
            n for n in nodes
            if n.node_type == "file" and NK.DOCKERFILE_BASE_IMAGES in n.metadata
        ]
        assert len(df_nodes) == 0

    def test_dockerfile_with_platform(self, tmp_path):
        repo = _make_repo_with_dockerfile(
            tmp_path,
            "FROM --platform=linux/amd64 python:3.12\nCMD [\"python\"]\n",
        )
        loader = FileSystemCodebaseLoader(
            organization_id=ORG_ID,
            plugins=[DockerfilePlugin()],
        )
        nodes, edges = loader.load(str(repo))

        df_nodes = [
            n for n in nodes
            if n.node_type == "file" and NK.DOCKERFILE_BASE_IMAGES in n.metadata
        ]
        assert len(df_nodes) == 1
        assert df_nodes[0].metadata[NK.DOCKERFILE_BASE_IMAGES] == "python:3.12"

    def test_named_dockerfile(self, tmp_path):
        repo = _make_repo_with_dockerfile(
            tmp_path,
            "FROM golang:1.22\nRUN go build\n",
            filename="Dockerfile.prod",
        )
        loader = FileSystemCodebaseLoader(
            organization_id=ORG_ID,
            plugins=[DockerfilePlugin()],
        )
        nodes, edges = loader.load(str(repo))

        df_nodes = [
            n for n in nodes
            if n.node_type == "file" and NK.DOCKERFILE_BASE_IMAGES in n.metadata
        ]
        assert len(df_nodes) == 1
        assert df_nodes[0].metadata[NK.DOCKERFILE_BASE_IMAGES] == "golang:1.22"


class TestDockerfilePluginSupportedLanguages:
    def test_universal_plugin(self):
        plugin = DockerfilePlugin()
        assert plugin.supported_languages() is None
