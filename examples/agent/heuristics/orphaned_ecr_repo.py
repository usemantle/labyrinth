"""Heuristic: ECR image repositories with no incoming `builds` edge."""

from __future__ import annotations

from labyrinth.graph.graph_models import NodeType

from ._base import BaseHeuristic, TerminalAction


class OrphanedEcrRepo(BaseHeuristic):
    name = "orphaned_ecr_repo"
    source_node_type = NodeType.IMAGE_REPOSITORY
    metadata_keys = {}  # no metadata filter — all ECR repos are candidates
    terminal_actions = [TerminalAction.MARK_EVALUATED, TerminalAction.CREATE_SOFT_LINK]
    skill_file = "link-dockerfile-to-ecr.md"

    def get_instructions(self) -> str:
        return (
            "This ECR image repository has no incoming `builds` edge from any Dockerfile. "
            "Search the graph for Dockerfiles that might build this image. Follow the "
            "investigation steps in the skill file."
        )
