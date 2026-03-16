"""Tests for stitch_dockerfile_entrypoints."""

from __future__ import annotations

import uuid

from src.graph.edges.contains_edge import ContainsEdge
from src.graph.graph_models import URN, Edge, EdgeType, Node, NodeMetadata, NodeMetadataKey, NodeType

from src.graph.stitching import stitch_dockerfile_entrypoints

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey


def _codebase_node(name="my-app"):
    return Node(
        organization_id=ORG_ID,
        urn=URN(f"urn:github:repo:org:::{name}"),
        node_type=NodeType.CODEBASE,
        metadata=NodeMetadata({NK.REPO_NAME: name}),
    )


def _file_node(codebase_name, rel_path, **extra_meta):
    meta = NodeMetadata({NK.FILE_PATH: rel_path})
    for k, v in extra_meta.items():
        meta[k] = v
    return Node(
        organization_id=ORG_ID,
        urn=URN(f"urn:github:repo:org:::{codebase_name}/{rel_path}"),
        parent_urn=URN(f"urn:github:repo:org:::{codebase_name}"),
        node_type=NodeType.FILE,
        metadata=meta,
    )


def _contains_edge(parent_urn, child_urn):
    return ContainsEdge.create(ORG_ID, parent_urn, child_urn)


class TestExecFormResolution:
    def test_cmd_python_exec_form(self):
        """CMD ["python", "src/main.py"] -> matches src/main.py FileNode."""
        codebase = _codebase_node()
        dockerfile = _file_node(
            "my-app", "Dockerfile",
            **{
                NK.DOCKERFILE_BASE_IMAGES: "python:3.12",
                NK.DOCKERFILE_CMD: '["python", "src/main.py"]',
            },
        )
        target = _file_node("my-app", "src/main.py")
        edges: list[Edge] = [
            _contains_edge(codebase.urn, dockerfile.urn),
            _contains_edge(codebase.urn, target.urn),
        ]

        _, result_edges = stitch_dockerfile_entrypoints(
            ORG_ID, [codebase, dockerfile, target], edges,
        )

        executes_edges = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes_edges) == 1
        assert executes_edges[0].from_urn == dockerfile.urn
        assert executes_edges[0].to_urn == target.urn
        assert executes_edges[0].metadata["confidence"] == 0.9
        assert executes_edges[0].metadata["detection_method"] == "static_parse"


class TestPythonModuleNotation:
    def test_uvicorn_module_notation(self):
        """CMD ["uvicorn", "app.main:app"] -> matches app/main.py."""
        codebase = _codebase_node()
        dockerfile = _file_node(
            "my-app", "Dockerfile",
            **{
                NK.DOCKERFILE_BASE_IMAGES: "python:3.12",
                NK.DOCKERFILE_CMD: '["uvicorn", "app.main:app"]',
            },
        )
        target = _file_node("my-app", "app/main.py")
        edges: list[Edge] = [
            _contains_edge(codebase.urn, dockerfile.urn),
            _contains_edge(codebase.urn, target.urn),
        ]

        _, result_edges = stitch_dockerfile_entrypoints(
            ORG_ID, [codebase, dockerfile, target], edges,
        )

        executes_edges = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes_edges) == 1
        assert executes_edges[0].to_urn == target.urn


class TestWorkdirCopyResolution:
    def test_workdir_with_absolute_entrypoint(self):
        """WORKDIR /app + CMD ["python", "/app/src/main.py"] -> src/main.py."""
        codebase = _codebase_node()
        dockerfile = _file_node(
            "my-app", "Dockerfile",
            **{
                NK.DOCKERFILE_BASE_IMAGES: "python:3.12",
                NK.DOCKERFILE_CMD: '["python", "/app/src/main.py"]',
                NK.DOCKERFILE_WORKDIR: "/app",
                NK.DOCKERFILE_COPY_TARGETS: ".",
            },
        )
        target = _file_node("my-app", "src/main.py")
        edges: list[Edge] = [
            _contains_edge(codebase.urn, dockerfile.urn),
            _contains_edge(codebase.urn, target.urn),
        ]

        _, result_edges = stitch_dockerfile_entrypoints(
            ORG_ID, [codebase, dockerfile, target], edges,
        )

        executes_edges = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes_edges) == 1
        assert executes_edges[0].to_urn == target.urn


class TestNoMatch:
    def test_no_matching_file(self):
        """No edge created when the entrypoint file doesn't exist in the codebase."""
        codebase = _codebase_node()
        dockerfile = _file_node(
            "my-app", "Dockerfile",
            **{
                NK.DOCKERFILE_BASE_IMAGES: "python:3.12",
                NK.DOCKERFILE_CMD: '["python", "nonexistent.py"]',
            },
        )
        edges: list[Edge] = [_contains_edge(codebase.urn, dockerfile.urn)]

        _, result_edges = stitch_dockerfile_entrypoints(
            ORG_ID, [codebase, dockerfile], edges,
        )

        executes_edges = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes_edges) == 0

    def test_variable_substitution_skipped(self):
        """No edge created when entrypoint uses variable substitution."""
        codebase = _codebase_node()
        dockerfile = _file_node(
            "my-app", "Dockerfile",
            **{
                NK.DOCKERFILE_BASE_IMAGES: "python:3.12",
                NK.DOCKERFILE_CMD: '["python", "${APP_MODULE}"]',
            },
        )
        target = _file_node("my-app", "src/main.py")
        edges: list[Edge] = [
            _contains_edge(codebase.urn, dockerfile.urn),
            _contains_edge(codebase.urn, target.urn),
        ]

        _, result_edges = stitch_dockerfile_entrypoints(
            ORG_ID, [codebase, dockerfile, target], edges,
        )

        executes_edges = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes_edges) == 0

    def test_shell_script_entrypoint_no_match(self):
        """Shell script entrypoints don't match .sh files (ambiguous)."""
        codebase = _codebase_node()
        dockerfile = _file_node(
            "my-app", "Dockerfile",
            **{
                NK.DOCKERFILE_BASE_IMAGES: "python:3.12",
                NK.DOCKERFILE_ENTRYPOINT: '["/docker-entrypoint.sh"]',
            },
        )
        # The .sh file exists but is at a different path
        script = _file_node("my-app", "docker-entrypoint.sh")
        edges: list[Edge] = [
            _contains_edge(codebase.urn, dockerfile.urn),
            _contains_edge(codebase.urn, script.urn),
        ]

        _, result_edges = stitch_dockerfile_entrypoints(
            ORG_ID, [codebase, dockerfile, script], edges,
        )

        # The absolute path /docker-entrypoint.sh resolves to docker-entrypoint.sh
        # which does exist, so an edge IS created (the file is found)
        executes_edges = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes_edges) == 1


class TestEntrypointPreferredOverCmd:
    def test_entrypoint_takes_precedence(self):
        """When both ENTRYPOINT and CMD exist, ENTRYPOINT is used."""
        codebase = _codebase_node()
        dockerfile = _file_node(
            "my-app", "Dockerfile",
            **{
                NK.DOCKERFILE_BASE_IMAGES: "python:3.12",
                NK.DOCKERFILE_ENTRYPOINT: '["python", "entrypoint.py"]',
                NK.DOCKERFILE_CMD: '["--flag"]',
            },
        )
        entrypoint_file = _file_node("my-app", "entrypoint.py")
        edges: list[Edge] = [
            _contains_edge(codebase.urn, dockerfile.urn),
            _contains_edge(codebase.urn, entrypoint_file.urn),
        ]

        _, result_edges = stitch_dockerfile_entrypoints(
            ORG_ID, [codebase, dockerfile, entrypoint_file], edges,
        )

        executes_edges = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes_edges) == 1
        assert executes_edges[0].to_urn == entrypoint_file.urn


class TestUvRunUvicorn:
    def test_uv_run_uvicorn_module_notation(self):
        """CMD ["uv", "run", "uvicorn", "main:app", ...] -> matches main.py."""
        codebase = _codebase_node()
        dockerfile = _file_node(
            "my-app", "Dockerfile",
            **{
                NK.DOCKERFILE_BASE_IMAGES: "python:3.13-slim",
                NK.DOCKERFILE_CMD: '["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]',
                NK.DOCKERFILE_WORKDIR: "/app",
                NK.DOCKERFILE_COPY_TARGETS: ".,.",
            },
        )
        target = _file_node("my-app", "main.py")
        edges: list[Edge] = [
            _contains_edge(codebase.urn, dockerfile.urn),
            _contains_edge(codebase.urn, target.urn),
        ]

        _, result_edges = stitch_dockerfile_entrypoints(
            ORG_ID, [codebase, dockerfile, target], edges,
        )

        executes_edges = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes_edges) == 1
        assert executes_edges[0].to_urn == target.urn


class TestShellFormResolution:
    def test_shell_form_cmd(self):
        """CMD python src/main.py (shell form) resolves correctly."""
        codebase = _codebase_node()
        dockerfile = _file_node(
            "my-app", "Dockerfile",
            **{
                NK.DOCKERFILE_BASE_IMAGES: "python:3.12",
                NK.DOCKERFILE_CMD: "python src/main.py",
            },
        )
        target = _file_node("my-app", "src/main.py")
        edges: list[Edge] = [
            _contains_edge(codebase.urn, dockerfile.urn),
            _contains_edge(codebase.urn, target.urn),
        ]

        _, result_edges = stitch_dockerfile_entrypoints(
            ORG_ID, [codebase, dockerfile, target], edges,
        )

        executes_edges = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes_edges) == 1
        assert executes_edges[0].to_urn == target.urn
