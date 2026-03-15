"""Heuristic: ECR image repositories with no incoming `builds` edge."""

from __future__ import annotations

from src.agent.heuristics._base import BaseHeuristic
from src.graph.graph_models import NodeType


class OrphanedEcrRepo(BaseHeuristic):
    name = "orphaned_ecr_repo"
    source_node_type = NodeType.IMAGE_REPOSITORY
    metadata_key = ""  # no metadata filter — all ECR repos are candidates
    skill_file = "link-dockerfile-to-ecr.md"

    @classmethod
    def get_instructions(cls) -> str:
        return (
            "This ECR image repository has no incoming `builds` edge from any Dockerfile. "
            "Search the graph for Dockerfiles that might build this image. Follow the "
            "investigation steps in the skill file."
        )
