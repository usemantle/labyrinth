"""Heuristic: Dependencies with known CVEs."""

from __future__ import annotations

from src.agent.heuristics._base import BaseHeuristic, TerminalAction
from src.graph.graph_models import NodeMetadataKey, NodeType


class VulnerableDependency(BaseHeuristic):
    name = "vulnerable_dependency"
    source_node_type = NodeType.DEPENDENCY
    metadata_keys = {NodeMetadataKey.CVE_IDS: True}
    terminal_actions = [TerminalAction.MARK_EVALUATED, TerminalAction.CREATE_PR]
    skill_file = "remediate-vulnerable-dependency.md"

    def get_instructions(self) -> str:
        return (
            "This dependency has known CVEs. Investigate the severity of the "
            "vulnerabilities, determine if a fixed version is available, and "
            "assess the blast radius within the codebase. Follow the skill file "
            "for investigation steps."
        )
