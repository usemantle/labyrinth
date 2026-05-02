"""Heuristic: Dockerfiles without a `builds` edge to an ECR repository."""

from __future__ import annotations

from src.agent.heuristics._base import BaseHeuristic, TerminalAction
from src.graph.graph_models import NodeMetadataKey, NodeType


class UnlinkedDockerfile(BaseHeuristic):
    name = "unlinked_dockerfile"
    source_node_type = NodeType.FILE
    metadata_keys = {NodeMetadataKey.DOCKERFILE_BASE_IMAGES: True}
    terminal_actions = [TerminalAction.MARK_EVALUATED, TerminalAction.CREATE_SOFT_LINK]
    skill_file = "link-dockerfile-to-ecr.md"

    def get_instructions(self) -> str:
        return (
            "This Dockerfile exists in the graph but has no outgoing `builds` edge to an "
            "image_repository node. Investigate whether this Dockerfile builds one of the "
            "ECR image repositories listed below. Follow the investigation steps in the "
            "skill file to determine the correct match and confidence level."
        )
