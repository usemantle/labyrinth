"""Stitcher: Dockerfile FILE nodes -> IMAGE_REPOSITORY nodes via BuildsEdge."""

from __future__ import annotations

import re
import uuid

from src.graph.edges.builds_edge import BuildsEdge
from src.graph.graph_models import (
    URN,
    EdgeMetadata,
    EdgeMetadataKey,
    Graph,
    Node,
    NodeMetadataKey,
    NodeType,
)
from src.graph.stitchers._base import Stitcher


class DockerfileToImageRepoStitcher(Stitcher):
    """Link Dockerfiles to their ECR image repositories via OCI labels or name heuristics."""

    def stitch(self, organization_id: uuid.UUID, graph: Graph, context: dict) -> Graph:
        NK = NodeMetadataKey
        result = Graph()

        idx = self.index_nodes(
            graph,
            types={NodeType.IMAGE_REPOSITORY, NodeType.IMAGE, NodeType.CODEBASE, NodeType.FILE},
        )

        # Build registries from indexed nodes
        image_repos: dict[str, URN] = {}
        image_oci_sources: dict[str, str] = {}
        codebase_by_url: dict[str, URN] = {}
        codebase_names: dict[str, URN] = {}
        dockerfiles: list[Node] = []
        dockerfile_by_codebase: dict[str, list[Node]] = {}

        for node in idx.nodes_of_type(NodeType.IMAGE_REPOSITORY):
            repo_name = node.metadata.get(NK.REPOSITORY_NAME, "")
            if repo_name:
                image_repos[repo_name] = node.urn

        for node in idx.nodes_of_type(NodeType.IMAGE):
            oci_source = node.metadata.get(NK.OCI_SOURCE)
            if oci_source and node.parent_urn:
                for rname, rurn in image_repos.items():
                    if rurn == node.parent_urn:
                        image_oci_sources[rname] = oci_source
                        break

        for node in idx.nodes_of_type(NodeType.CODEBASE):
            repo_url = node.metadata.get(NK.REPO_URL, "")
            clone_url = node.metadata.get(NK.CLONE_URL, "")
            repo_name = node.metadata.get(NK.REPO_NAME, "")
            if repo_url:
                codebase_by_url[_normalize_url(repo_url)] = node.urn
            if clone_url:
                codebase_by_url[_normalize_url(clone_url)] = node.urn
            if repo_name:
                codebase_names[repo_name] = node.urn

        for node in idx.nodes_of_type(NodeType.FILE):
            if NK.DOCKERFILE_BASE_IMAGES in node.metadata:
                dockerfiles.append(node)

        for df_node in dockerfiles:
            parent = df_node.parent_urn
            if parent:
                dockerfile_by_codebase.setdefault(str(parent), []).append(df_node)

        if not image_repos or not dockerfiles:
            return result

        linked_repos: set[str] = set()

        # Strategy 1: OCI label matching (confidence 1.0)
        for repo_name, oci_source in image_oci_sources.items():
            normalized = _normalize_url(oci_source)
            codebase_urn = codebase_by_url.get(normalized)
            if codebase_urn is None:
                continue

            repo_urn = image_repos[repo_name]
            dfs = dockerfile_by_codebase.get(str(codebase_urn), [])
            for df_node in dfs:
                result.edges.append(BuildsEdge.create(
                    organization_id,
                    df_node.urn,
                    repo_urn,
                    metadata=EdgeMetadata({
                        EdgeMetadataKey.DETECTION_METHOD: "oci_label",
                        EdgeMetadataKey.CONFIDENCE: 1.0,
                    }),
                ))
            linked_repos.add(repo_name)

        # Strategy 2: Name heuristic (confidence 0.8)
        for repo_name, repo_urn in image_repos.items():
            if repo_name in linked_repos:
                continue

            base_name = repo_name.rsplit("/", 1)[-1]
            codebase_urn = codebase_names.get(base_name)
            if codebase_urn is None:
                stripped = re.sub(r"[-_](api|app|service|server|web)$", "", base_name)
                codebase_urn = codebase_names.get(stripped)

            if codebase_urn is None:
                continue

            dfs = dockerfile_by_codebase.get(str(codebase_urn), [])
            for df_node in dfs:
                result.edges.append(BuildsEdge.create(
                    organization_id,
                    df_node.urn,
                    repo_urn,
                    metadata=EdgeMetadata({
                        EdgeMetadataKey.DETECTION_METHOD: "name_heuristic",
                        EdgeMetadataKey.CONFIDENCE: 0.8,
                    }),
                ))

        return result


def _normalize_url(url: str) -> str:
    """Normalize a git/repository URL for comparison."""
    url = url.rstrip("/")
    url = re.sub(r"\.git$", "", url)
    url = re.sub(r"^https?://", "", url)
    url = re.sub(r"^git@([^:]+):", r"\1/", url)
    return url.lower()
