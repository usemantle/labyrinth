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

from src.graph.edges.contains_edge import ContainsEdge
from src.graph.graph_models import Edge, Node, NodeMetadataKey, NodeType
from src.graph.loaders.codebase.plugins._base import CodebasePlugin
from src.graph.nodes.file_node import FileNode

if TYPE_CHECKING:
    from src.graph.loaders.codebase.codebase_loader import PostProcessContext

NK = NodeMetadataKey

# Patterns for Dockerfile discovery
_DOCKERFILE_GLOBS = ["**/Dockerfile", "**/Dockerfile.*", "**/*.dockerfile"]

# Regex for FROM directive (handles multi-stage builds)
_FROM_RE = re.compile(
    r"^\s*FROM\s+(?:--platform=\S+\s+)?(\S+?)(?:\s+[Aa][Ss]\s+\S+)?\s*$",
    re.MULTILINE,
)

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

        return nodes, edges
