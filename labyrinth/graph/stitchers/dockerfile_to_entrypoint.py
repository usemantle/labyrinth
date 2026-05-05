"""Stitcher: Dockerfile FILE nodes -> code FILE nodes via ExecutesEdge."""

from __future__ import annotations

import json
import uuid

from labyrinth.graph.edges.executes_edge import ExecutesEdge
from labyrinth.graph.graph_models import (
    URN,
    EdgeMetadata,
    EdgeMetadataKey,
    Graph,
    Node,
    NodeMetadataKey,
    NodeType,
)
from labyrinth.graph.stitchers._base import Stitcher

# Runners/interpreters to skip when extracting the file argument
_KNOWN_RUNNERS = frozenset({
    "python", "python3", "node", "uvicorn", "gunicorn",
    "java", "npm", "sh", "bash", "ruby", "perl", "php",
    "dotnet", "go", "run", "exec", "deno", "uv", "npx",
    "poetry", "pipenv", "conda",
})


class DockerfileToEntrypointStitcher(Stitcher):
    """Link Dockerfiles to the code files they execute via ENTRYPOINT/CMD."""

    def stitch(self, organization_id: uuid.UUID, graph: Graph, context: dict) -> Graph:
        NK = NodeMetadataKey
        result = Graph()

        idx = self.index_nodes(graph, types={NodeType.FILE})

        dockerfiles: list[Node] = []
        file_nodes_by_codebase: dict[str, dict[str, URN]] = {}

        for node in idx.nodes_of_type(NodeType.FILE):
            has_entrypoint = NK.DOCKERFILE_ENTRYPOINT in node.metadata
            has_cmd = NK.DOCKERFILE_CMD in node.metadata
            if has_entrypoint or has_cmd:
                dockerfiles.append(node)

            if node.parent_urn and NK.FILE_PATH in node.metadata:
                codebase_key = str(node.parent_urn)
                file_nodes_by_codebase.setdefault(codebase_key, {})
                rel_path = node.metadata[NK.FILE_PATH]
                file_nodes_by_codebase[codebase_key][rel_path] = node.urn

        if not dockerfiles:
            return result

        for df_node in dockerfiles:
            raw = df_node.metadata.get(NK.DOCKERFILE_ENTRYPOINT)
            if not raw:
                raw = df_node.metadata.get(NK.DOCKERFILE_CMD)
            if not raw:
                continue

            target_file = _extract_target_file(raw)
            if not target_file:
                continue

            workdir = df_node.metadata.get(NK.DOCKERFILE_WORKDIR)
            copy_targets_raw = df_node.metadata.get(NK.DOCKERFILE_COPY_TARGETS, "")
            copy_targets = [t for t in copy_targets_raw.split(",") if t] if copy_targets_raw else []

            resolved_path = _resolve_container_path_to_codebase(
                target_file, workdir, copy_targets,
            )

            if not resolved_path:
                continue

            codebase_key = str(df_node.parent_urn) if df_node.parent_urn else None
            if not codebase_key:
                continue

            file_registry = file_nodes_by_codebase.get(codebase_key, {})
            target_urn = file_registry.get(resolved_path)

            if target_urn:
                result.edges.append(ExecutesEdge.create(
                    organization_id,
                    df_node.urn,
                    target_urn,
                    metadata=EdgeMetadata({
                        EdgeMetadataKey.DETECTION_METHOD: "static_parse",
                        EdgeMetadataKey.CONFIDENCE: 0.9,
                    }),
                ))

        return result


def _parse_exec_form(raw: str) -> list[str]:
    """Parse a JSON exec-form instruction like '["python", "src/main.py"]'."""
    raw = raw.strip()
    if raw.startswith("["):
        try:
            parts = json.loads(raw)
            if isinstance(parts, list):
                return [str(p) for p in parts]
        except (json.JSONDecodeError, ValueError):
            pass
    return []


def _parse_shell_form(raw: str) -> list[str]:
    """Parse a shell-form instruction like 'python src/main.py'."""
    return raw.strip().split()


def _extract_target_file(raw_instruction: str) -> str | None:
    """Extract the target file path from an ENTRYPOINT or CMD instruction."""
    parts = _parse_exec_form(raw_instruction)
    if not parts:
        parts = _parse_shell_form(raw_instruction)
    if not parts:
        return None

    file_arg = None
    for part in parts:
        if part.startswith("-"):
            continue
        basename = part.rsplit("/", 1)[-1]
        if basename in _KNOWN_RUNNERS:
            continue
        file_arg = part
        break

    if not file_arg:
        return None

    if "$" in file_arg or "%" in file_arg:
        return None

    # Handle Python module notation: app.main:app -> app/main.py
    if ":" in file_arg and not file_arg.startswith("/"):
        module_part = file_arg.split(":")[0]
        if "/" not in module_part:
            file_arg = module_part.replace(".", "/") + ".py"

    return file_arg


def _resolve_container_path_to_codebase(
    file_arg: str,
    workdir: str | None,
    copy_targets: list[str],
) -> str:
    """Map a container-internal path back to a codebase-relative path."""
    path = file_arg.lstrip("/")

    if not file_arg.startswith("/"):
        return path

    if workdir:
        wd = workdir.strip("/")
        if path.startswith(wd + "/"):
            return path[len(wd) + 1:]
        if path == wd:
            return ""

    for dest in copy_targets:
        dest_norm = dest.strip("/")
        if dest_norm and path.startswith(dest_norm + "/"):
            return path[len(dest_norm) + 1:]

    return path
