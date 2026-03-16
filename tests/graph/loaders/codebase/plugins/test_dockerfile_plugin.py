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


class TestDockerfileEntrypointExtraction:
    def test_entrypoint_exec_form(self, tmp_path):
        repo = _make_repo_with_dockerfile(
            tmp_path,
            'FROM python:3.12\nENTRYPOINT ["python", "src/main.py"]\n',
        )
        loader = FileSystemCodebaseLoader(
            organization_id=ORG_ID,
            plugins=[DockerfilePlugin()],
        )
        nodes, _ = loader.load(str(repo))
        df_nodes = [n for n in nodes if NK.DOCKERFILE_ENTRYPOINT in n.metadata]
        assert len(df_nodes) == 1
        assert df_nodes[0].metadata[NK.DOCKERFILE_ENTRYPOINT] == '["python", "src/main.py"]'

    def test_entrypoint_shell_form(self, tmp_path):
        repo = _make_repo_with_dockerfile(
            tmp_path,
            "FROM python:3.12\nENTRYPOINT python src/main.py\n",
        )
        loader = FileSystemCodebaseLoader(
            organization_id=ORG_ID,
            plugins=[DockerfilePlugin()],
        )
        nodes, _ = loader.load(str(repo))
        df_nodes = [n for n in nodes if NK.DOCKERFILE_ENTRYPOINT in n.metadata]
        assert len(df_nodes) == 1
        assert df_nodes[0].metadata[NK.DOCKERFILE_ENTRYPOINT] == "python src/main.py"

    def test_cmd_extraction(self, tmp_path):
        repo = _make_repo_with_dockerfile(
            tmp_path,
            'FROM python:3.12\nCMD ["uvicorn", "app.main:app"]\n',
        )
        loader = FileSystemCodebaseLoader(
            organization_id=ORG_ID,
            plugins=[DockerfilePlugin()],
        )
        nodes, _ = loader.load(str(repo))
        df_nodes = [n for n in nodes if NK.DOCKERFILE_CMD in n.metadata]
        assert len(df_nodes) == 1
        assert df_nodes[0].metadata[NK.DOCKERFILE_CMD] == '["uvicorn", "app.main:app"]'

    def test_workdir_extraction(self, tmp_path):
        repo = _make_repo_with_dockerfile(
            tmp_path,
            "FROM python:3.12\nWORKDIR /app\nCOPY . .\nCMD python main.py\n",
        )
        loader = FileSystemCodebaseLoader(
            organization_id=ORG_ID,
            plugins=[DockerfilePlugin()],
        )
        nodes, _ = loader.load(str(repo))
        df_nodes = [n for n in nodes if NK.DOCKERFILE_WORKDIR in n.metadata]
        assert len(df_nodes) == 1
        assert df_nodes[0].metadata[NK.DOCKERFILE_WORKDIR] == "/app"

    def test_copy_targets(self, tmp_path):
        repo = _make_repo_with_dockerfile(
            tmp_path,
            "FROM python:3.12\nCOPY requirements.txt /app/\nCOPY src/ /app/src/\nCMD python main.py\n",
        )
        loader = FileSystemCodebaseLoader(
            organization_id=ORG_ID,
            plugins=[DockerfilePlugin()],
        )
        nodes, _ = loader.load(str(repo))
        df_nodes = [n for n in nodes if NK.DOCKERFILE_COPY_TARGETS in n.metadata]
        assert len(df_nodes) == 1
        targets = df_nodes[0].metadata[NK.DOCKERFILE_COPY_TARGETS]
        assert "/app/" in targets
        assert "/app/src/" in targets

    def test_multistage_only_final_stage(self, tmp_path):
        repo = _make_repo_with_dockerfile(
            tmp_path,
            "FROM node:20 AS builder\nWORKDIR /build\nCMD npm run build\n"
            "FROM python:3.12\nWORKDIR /app\nCOPY . .\nCMD python serve.py\n",
        )
        loader = FileSystemCodebaseLoader(
            organization_id=ORG_ID,
            plugins=[DockerfilePlugin()],
        )
        nodes, _ = loader.load(str(repo))
        df_nodes = [n for n in nodes if NK.DOCKERFILE_CMD in n.metadata]
        assert len(df_nodes) == 1
        # Should have the final stage CMD, not the builder CMD
        assert df_nodes[0].metadata[NK.DOCKERFILE_CMD] == "python serve.py"
        assert df_nodes[0].metadata[NK.DOCKERFILE_WORKDIR] == "/app"

    def test_variable_substitution_stored_raw(self, tmp_path):
        repo = _make_repo_with_dockerfile(
            tmp_path,
            'FROM python:3.12\nCMD ["python", "${APP_MODULE}"]\n',
        )
        loader = FileSystemCodebaseLoader(
            organization_id=ORG_ID,
            plugins=[DockerfilePlugin()],
        )
        nodes, _ = loader.load(str(repo))
        df_nodes = [n for n in nodes if NK.DOCKERFILE_CMD in n.metadata]
        assert len(df_nodes) == 1
        assert "${APP_MODULE}" in df_nodes[0].metadata[NK.DOCKERFILE_CMD]


class TestDockerfilePluginSupportedLanguages:
    def test_universal_plugin(self):
        plugin = DockerfilePlugin()
        assert plugin.supported_languages() is None
