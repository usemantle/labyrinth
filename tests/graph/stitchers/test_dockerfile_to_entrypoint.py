"""Tests for DockerfileToEntrypointStitcher."""

from __future__ import annotations

import uuid

from src.graph.edges.contains_edge import ContainsEdge
from src.graph.graph_models import URN, Edge, EdgeType, Graph, Node, NodeMetadata, NodeMetadataKey, NodeType
from src.graph.stitchers.dockerfile_to_entrypoint import DockerfileToEntrypointStitcher

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


def _stitch(nodes, edges):
    graph = Graph(nodes=nodes, edges=edges)
    result = DockerfileToEntrypointStitcher().stitch(ORG_ID, graph, {})
    return result.edges


class TestExecFormResolution:
    def test_cmd_python_exec_form(self):
        codebase = _codebase_node()
        dockerfile = _file_node(
            "my-app", "Dockerfile",
            **{NK.DOCKERFILE_BASE_IMAGES: "python:3.12", NK.DOCKERFILE_CMD: '["python", "src/main.py"]'},
        )
        target = _file_node("my-app", "src/main.py")
        edges = [_contains_edge(codebase.urn, dockerfile.urn), _contains_edge(codebase.urn, target.urn)]

        result_edges = _stitch([codebase, dockerfile, target], edges)
        executes = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes) == 1
        assert executes[0].from_urn == dockerfile.urn
        assert executes[0].to_urn == target.urn
        assert executes[0].metadata["confidence"] == 0.9


class TestPythonModuleNotation:
    def test_uvicorn_module_notation(self):
        codebase = _codebase_node()
        dockerfile = _file_node(
            "my-app", "Dockerfile",
            **{NK.DOCKERFILE_BASE_IMAGES: "python:3.12", NK.DOCKERFILE_CMD: '["uvicorn", "app.main:app"]'},
        )
        target = _file_node("my-app", "app/main.py")
        edges = [_contains_edge(codebase.urn, dockerfile.urn), _contains_edge(codebase.urn, target.urn)]

        result_edges = _stitch([codebase, dockerfile, target], edges)
        executes = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes) == 1
        assert executes[0].to_urn == target.urn


class TestWorkdirCopyResolution:
    def test_workdir_with_absolute_entrypoint(self):
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
        edges = [_contains_edge(codebase.urn, dockerfile.urn), _contains_edge(codebase.urn, target.urn)]

        result_edges = _stitch([codebase, dockerfile, target], edges)
        executes = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes) == 1
        assert executes[0].to_urn == target.urn


class TestNoMatch:
    def test_no_matching_file(self):
        codebase = _codebase_node()
        dockerfile = _file_node(
            "my-app", "Dockerfile",
            **{NK.DOCKERFILE_BASE_IMAGES: "python:3.12", NK.DOCKERFILE_CMD: '["python", "nonexistent.py"]'},
        )
        edges = [_contains_edge(codebase.urn, dockerfile.urn)]

        result_edges = _stitch([codebase, dockerfile], edges)
        executes = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes) == 0

    def test_variable_substitution_skipped(self):
        codebase = _codebase_node()
        dockerfile = _file_node(
            "my-app", "Dockerfile",
            **{NK.DOCKERFILE_BASE_IMAGES: "python:3.12", NK.DOCKERFILE_CMD: '["python", "${APP_MODULE}"]'},
        )
        target = _file_node("my-app", "src/main.py")
        edges = [_contains_edge(codebase.urn, dockerfile.urn), _contains_edge(codebase.urn, target.urn)]

        result_edges = _stitch([codebase, dockerfile, target], edges)
        executes = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes) == 0

    def test_shell_script_entrypoint_no_match(self):
        codebase = _codebase_node()
        dockerfile = _file_node(
            "my-app", "Dockerfile",
            **{NK.DOCKERFILE_BASE_IMAGES: "python:3.12", NK.DOCKERFILE_ENTRYPOINT: '["/docker-entrypoint.sh"]'},
        )
        script = _file_node("my-app", "docker-entrypoint.sh")
        edges = [_contains_edge(codebase.urn, dockerfile.urn), _contains_edge(codebase.urn, script.urn)]

        result_edges = _stitch([codebase, dockerfile, script], edges)
        executes = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes) == 1


class TestEntrypointPreferredOverCmd:
    def test_entrypoint_takes_precedence(self):
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
        edges = [_contains_edge(codebase.urn, dockerfile.urn), _contains_edge(codebase.urn, entrypoint_file.urn)]

        result_edges = _stitch([codebase, dockerfile, entrypoint_file], edges)
        executes = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes) == 1
        assert executes[0].to_urn == entrypoint_file.urn


class TestUvRunUvicorn:
    def test_uv_run_uvicorn_module_notation(self):
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
        edges = [_contains_edge(codebase.urn, dockerfile.urn), _contains_edge(codebase.urn, target.urn)]

        result_edges = _stitch([codebase, dockerfile, target], edges)
        executes = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes) == 1
        assert executes[0].to_urn == target.urn


class TestShellFormResolution:
    def test_shell_form_cmd(self):
        codebase = _codebase_node()
        dockerfile = _file_node(
            "my-app", "Dockerfile",
            **{NK.DOCKERFILE_BASE_IMAGES: "python:3.12", NK.DOCKERFILE_CMD: "python src/main.py"},
        )
        target = _file_node("my-app", "src/main.py")
        edges = [_contains_edge(codebase.urn, dockerfile.urn), _contains_edge(codebase.urn, target.urn)]

        result_edges = _stitch([codebase, dockerfile, target], edges)
        executes = [e for e in result_edges if e.edge_type == EdgeType.EXECUTES]
        assert len(executes) == 1
        assert executes[0].to_urn == target.urn
