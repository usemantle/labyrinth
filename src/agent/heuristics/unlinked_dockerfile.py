"""Heuristic: Dockerfiles without a `builds` edge to an ECR repository."""

from __future__ import annotations

from src.agent.heuristics._base import BaseHeuristic
from src.graph.graph_models import EdgeType, NodeType


class UnlinkedDockerfile(BaseHeuristic):
    name = "unlinked_dockerfile"
    source_node_type = NodeType.FILE
    metadata_key = "dockerfile_base_images"
    target_edge_type = EdgeType.BUILDS
    target_node_type = NodeType.IMAGE_REPOSITORY
    skill_file = "link-dockerfile-to-ecr.md"

    @classmethod
    def get_instructions(cls) -> str:
        return (
            "This Dockerfile exists in the graph but has no outgoing `builds` edge to an "
            "image_repository node. Investigate whether this Dockerfile builds one of the "
            "ECR image repositories listed below. Follow the investigation steps in the "
            "skill file to determine the correct match and confidence level."
        )
