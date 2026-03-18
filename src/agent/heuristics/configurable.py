"""Concrete, serializable heuristic — no subclassing required."""

from __future__ import annotations

from typing import Literal

from src.agent.heuristics._base import BaseHeuristic, TerminalAction


class ConfigurableHeuristic(BaseHeuristic):
    """A heuristic that can be instantiated from data without subclassing.

    Supports the same AND/OR metadata key logic as the base class.
    Skill content is stored inline (``skill_content``) rather than as a file path.
    """

    def __init__(
        self,
        name: str,
        source_node_type: str,
        metadata_keys: list[str],
        terminal_actions: list[TerminalAction],
        metadata_key_op: Literal["AND", "OR"] = "OR",
        instructions: str = "",
        skill_content: str = "",
    ) -> None:
        self.name = name
        self.source_node_type = source_node_type
        self.metadata_keys = metadata_keys
        self.terminal_actions = terminal_actions
        self.metadata_key_op = metadata_key_op
        self.instructions = instructions
        self.skill_content = skill_content

    def get_instructions(self) -> str:
        if self.instructions:
            return self.instructions
        op_label = f"({self.metadata_key_op})"
        keys_str = ", ".join(self.metadata_keys) if self.metadata_keys else "any node"
        return (
            f"Investigate {self.source_node_type} nodes where "
            f"metadata keys {op_label} match: {keys_str}."
        )

    def get_playbook(self) -> str | None:
        return self.skill_content or super().get_playbook()

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "source_node_type": self.source_node_type,
            "metadata_keys": self.metadata_keys,
            "metadata_key_op": self.metadata_key_op,
            "terminal_actions": [str(a) for a in self.terminal_actions],
            "instructions": self.instructions,
            "skill_content": self.skill_content,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ConfigurableHeuristic":
        return cls(
            name=data["name"],
            source_node_type=data["source_node_type"],
            metadata_keys=data.get("metadata_keys", []),
            terminal_actions=[
                TerminalAction(a)
                for a in data.get("terminal_actions", ["mark_evaluated"])
            ],
            metadata_key_op=data.get("metadata_key_op", "OR"),
            instructions=data.get("instructions", ""),
            skill_content=data.get("skill_content", ""),
        )
