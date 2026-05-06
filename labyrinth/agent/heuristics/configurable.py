"""Concrete, serializable heuristic — no subclassing required."""

from __future__ import annotations

from typing import Literal

from labyrinth.agent.heuristics._base import BaseHeuristic, MetadataFilter, TerminalAction


class ConfigurableHeuristic(BaseHeuristic):
    """A heuristic that can be instantiated from data without subclassing.

    ``metadata_keys`` and ``dest_node_metadata`` are filter dicts: a value of
    ``True`` means "key must be present", a string value means "key present
    AND equal to this value". See :class:`BaseHeuristic` for the full
    semantics, including the optional ``dest_node_type`` path-linking mode.

    The ``instructions`` text is the agent prompt; there is no separate
    inline-skill field — anything previously kept in ``skill_content`` should
    be merged into ``instructions``.
    """

    def __init__(
        self,
        name: str,
        source_node_type: str,
        metadata_keys: MetadataFilter,
        terminal_actions: list[TerminalAction],
        metadata_key_op: Literal["AND", "OR"] = "OR",
        instructions: str = "",
        dest_node_type: str | None = None,
        dest_node_metadata: MetadataFilter | None = None,
        dest_metadata_key_op: Literal["AND", "OR"] = "OR",
    ) -> None:
        self.name = name
        self.source_node_type = source_node_type
        self.metadata_keys = metadata_keys or {}
        self.terminal_actions = terminal_actions
        self.metadata_key_op = metadata_key_op
        self.instructions = instructions
        self.dest_node_type = dest_node_type
        self.dest_node_metadata = dest_node_metadata or {}
        self.dest_metadata_key_op = dest_metadata_key_op

    def get_instructions(self) -> str:
        if self.instructions:
            return self.instructions
        op_label = f"({self.metadata_key_op})"
        keys_str = (
            ", ".join(_describe_filter(self.metadata_keys))
            if self.metadata_keys else "any node"
        )
        base = (
            f"Investigate {self.source_node_type} nodes where "
            f"metadata keys {op_label} match: {keys_str}."
        )
        if self.dest_node_type:
            dest_op = f"({self.dest_metadata_key_op})"
            dest_str = (
                ", ".join(_describe_filter(self.dest_node_metadata))
                if self.dest_node_metadata else "any node"
            )
            base += (
                f" Linked path-target: {self.dest_node_type} where "
                f"metadata keys {dest_op} match: {dest_str}."
            )
        return base

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "source_node_type": self.source_node_type,
            "metadata_keys": self.metadata_keys,
            "metadata_key_op": self.metadata_key_op,
            "terminal_actions": [str(a) for a in self.terminal_actions],
            "instructions": self.instructions,
            "dest_node_type": self.dest_node_type,
            "dest_node_metadata": self.dest_node_metadata,
            "dest_metadata_key_op": self.dest_metadata_key_op,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ConfigurableHeuristic":
        return cls(
            name=data["name"],
            source_node_type=data["source_node_type"],
            metadata_keys=data.get("metadata_keys", {}),
            terminal_actions=[
                TerminalAction(a)
                for a in data.get("terminal_actions", ["mark_evaluated"])
            ],
            metadata_key_op=data.get("metadata_key_op", "OR"),
            instructions=data.get("instructions", ""),
            dest_node_type=data.get("dest_node_type"),
            dest_node_metadata=data.get("dest_node_metadata", {}),
            dest_metadata_key_op=data.get("dest_metadata_key_op", "OR"),
        )


def _describe_filter(filters: MetadataFilter) -> list[str]:
    """Render a filter dict as ``key`` (presence) or ``key=value`` (exact match)."""
    return [k if v is True else f"{k}={v}" for k, v in filters.items()]
