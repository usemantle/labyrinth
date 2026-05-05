"""
Dockerfile plugin for the codebase loader.

Discovers Dockerfiles during post-processing, parses FROM and LABEL
directives, and tags the corresponding FileNode with base image and
OCI label metadata for downstream stitching.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.graph_models import Edge, Node, NodeMetadataKey, NodeType
from labyrinth.graph.loaders.codebase.plugins._base import CodebasePlugin
from labyrinth.graph.nodes.file_node import FileNode

if TYPE_CHECKING:
    from labyrinth.graph.loaders.codebase.codebase_loader import PostProcessContext

NK = NodeMetadataKey

# Patterns for Dockerfile discovery
_DOCKERFILE_GLOBS = ["**/Dockerfile", "**/Dockerfile.*", "**/*.dockerfile"]

# Regex for FROM directive (handles multi-stage builds)
_FROM_RE = re.compile(
    r"^\s*FROM\s+(?:--platform=\S+\s+)?(\S+?)(?:\s+[Aa][Ss]\s+\S+)?\s*$",
    re.MULTILINE,
)

# Regex for ENTRYPOINT, CMD, WORKDIR, COPY directives
_ENTRYPOINT_RE = re.compile(r"^\s*ENTRYPOINT\s+(.*)", re.MULTILINE)
_CMD_RE = re.compile(r"^\s*CMD\s+(.*)", re.MULTILINE)
_WORKDIR_RE = re.compile(r"^\s*WORKDIR\s+(\S+)", re.MULTILINE)
_COPY_RE = re.compile(r"^\s*COPY\s+(?:--[^\s]+\s+)*(.+?)\s+(\S+)\s*$", re.MULTILINE)

# Regex for LABEL directives (key=value or key="value")
_LABEL_RE = re.compile(
    r'^\s*LABEL\s+(.*)',
    re.MULTILINE,
)

# Regex for individual label key=value pairs
_LABEL_KV_RE = re.compile(
    r'(\S+?)=(?:"([^"]*?)"|(\S+))',
)


def _parse_base_images(content: str) -> list[str]:
    """Extract base image references from FROM directives."""
    return _FROM_RE.findall(content)


def _parse_labels(content: str) -> dict[str, str]:
    """Extract LABEL key=value pairs from Dockerfile content."""
    labels: dict[str, str] = {}
    for match in _LABEL_RE.finditer(content):
        label_line = match.group(1)
        # Handle line continuations
        label_line = label_line.replace("\\\n", " ")
        for kv_match in _LABEL_KV_RE.finditer(label_line):
            key = kv_match.group(1)
            value = kv_match.group(2) if kv_match.group(2) is not None else kv_match.group(3)
            labels[key] = value
    return labels


def _extract_final_stage(content: str) -> str:
    """Return content after the last FROM line (final stage of multi-stage build)."""
    lines = content.split("\n")
    last_from_idx = 0
    for i, line in enumerate(lines):
        if re.match(r"^\s*FROM\s+", line, re.IGNORECASE):
            last_from_idx = i
    return "\n".join(lines[last_from_idx:])


def _parse_entrypoint(content: str) -> str | None:
    """Extract the ENTRYPOINT instruction from the final stage."""
    final = _extract_final_stage(content)
    match = _ENTRYPOINT_RE.search(final)
    return match.group(1).strip() if match else None


def _parse_cmd(content: str) -> str | None:
    """Extract the CMD instruction from the final stage."""
    final = _extract_final_stage(content)
    match = _CMD_RE.search(final)
    return match.group(1).strip() if match else None


def _parse_workdir(content: str) -> str | None:
    """Extract the last WORKDIR from the final stage."""
    final = _extract_final_stage(content)
    matches = _WORKDIR_RE.findall(final)
    return matches[-1] if matches else None


def _parse_copy_targets(content: str) -> list[str]:
    """Extract COPY destination paths from the final stage."""
    final = _extract_final_stage(content)
    targets: list[str] = []
    for match in _COPY_RE.finditer(final):
        targets.append(match.group(2))
    return targets


class DockerfilePlugin(CodebasePlugin):
    """Discovers Dockerfiles and extracts base images and OCI labels."""

    @classmethod
    def auto_detect(cls, root_path):
        for pattern in _DOCKERFILE_GLOBS:
            if any(root_path.glob(pattern)):
                return True
        return False

    def supported_languages(self) -> set[str] | None:
        return None  # Universal plugin — runs for any codebase

    def post_process(
        self,
        nodes: list[Node],
        edges: list[Edge],
        context: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        # Build lookup from relative path -> node
        file_nodes_by_path: dict[str, Node] = {}
        for node in nodes:
            if NK.FILE_PATH in node.metadata and node.node_type == NodeType.FILE:
                file_nodes_by_path[node.metadata[NK.FILE_PATH]] = node

        # Find the codebase root URN (first codebase node)
        codebase_urn = None
        for node in nodes:
            if node.node_type == NodeType.CODEBASE:
                codebase_urn = node.urn
                break

        # Find Dockerfiles
        root = context.root_path
        dockerfile_paths: set[Path] = set()
        for pattern in _DOCKERFILE_GLOBS:
            dockerfile_paths.update(root.glob(pattern))

        for df_path in sorted(dockerfile_paths):
            rel_path = str(df_path.relative_to(root))
            try:
                content = df_path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue

            base_images = _parse_base_images(content)
            if not base_images:
                continue

            # Find existing FileNode or create one
            file_node = file_nodes_by_path.get(rel_path)
            if file_node is None and codebase_urn is not None:
                file_urn = context.build_urn(context.root_name, rel_path)
                file_node = FileNode.create(
                    context.organization_id,
                    file_urn,
                    codebase_urn,
                    file_path=rel_path,
                    language="dockerfile",
                    size_bytes=df_path.stat().st_size,
                )
                nodes.append(file_node)
                edges.append(ContainsEdge.create(
                    context.organization_id, codebase_urn, file_urn,
                ))
                file_nodes_by_path[rel_path] = file_node

            if file_node is not None:
                file_node.metadata[NK.DOCKERFILE_BASE_IMAGES] = ",".join(base_images)

                # Extract entrypoint-related metadata
                entrypoint = _parse_entrypoint(content)
                cmd = _parse_cmd(content)
                workdir = _parse_workdir(content)
                copy_targets = _parse_copy_targets(content)

                if entrypoint:
                    file_node.metadata[NK.DOCKERFILE_ENTRYPOINT] = entrypoint
                if cmd:
                    file_node.metadata[NK.DOCKERFILE_CMD] = cmd
                if workdir:
                    file_node.metadata[NK.DOCKERFILE_WORKDIR] = workdir
                if copy_targets:
                    file_node.metadata[NK.DOCKERFILE_COPY_TARGETS] = ",".join(copy_targets)

        return nodes, edges
