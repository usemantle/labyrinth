"""Base class for all heuristics."""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal

from src.agent.candidates import Candidate, candidate_id
from src.mcp.graph_store import GraphStore

SKILL_DIR = Path(__file__).resolve().parent.parent / "skills"


class TerminalAction(enum.StrEnum):
    MARK_EVALUATED = "mark_evaluated"
    CREATE_SOFT_LINK = "create_soft_link"
    CREATE_PR = "create_pr"


TERMINAL_ACTION_PROMPTS: dict[TerminalAction, str] = {
    TerminalAction.MARK_EVALUATED: (
        "Call `update_node_metadata` with:\n"
        "- urn: `{source_urn}`\n"
        '- metadata: a JSON object including `"{heuristic_name}_last_evaluated_at"` '
        "set to the current ISO timestamp, plus any finding/risk fields documented "
        "in the playbook."
    ),
    TerminalAction.CREATE_SOFT_LINK: (
        "If evidence supports a relationship, call `add_soft_link` with appropriate "
        "edge_type, confidence, and note. If evidence is insufficient, explain your "
        "reasoning."
    ),
    TerminalAction.CREATE_PR: (
        "If a fix is available, create a branch and open a pull request to remediate "
        "the issue using the GitHub MCP tools (create_or_update_file, create_branch, "
        "create_pull_request)."
    ),
}

TERMINAL_ACTION_MCP_SERVERS: dict[TerminalAction, dict] = {
    TerminalAction.CREATE_PR: {
        "github": {
            "command": "npx",
            "args": ["-y", "@modelcontextprotocol/server-github"],
        },
    },
}


class BaseHeuristic(ABC):
    """A heuristic detects candidate nodes worth investigating.

    Subclasses define what to look for (source node type, metadata keys),
    the terminal actions, and how to investigate (instructions + optional playbook).

    The default ``find()`` implementation covers the common pattern:
    iterate all nodes of ``source_node_type``, check for ``metadata_keys``
    presence (AND/OR logic), and emit a ``Candidate`` for each match.  Override
    ``find()`` for heuristics that need custom logic.
    """

    # ── Identity ──
    name: str  # unique key, e.g. "unlinked_dockerfile"

    # ── What to search ──
    source_node_type: str  # e.g. "file", "function", "class"
    metadata_keys: list[str] = []  # metadata keys whose presence triggers this heuristic
    metadata_key_op: Literal["AND", "OR"] = "OR"  # how to combine multiple keys

    # ── Terminal actions ──
    terminal_actions: list[TerminalAction] = [TerminalAction.MARK_EVALUATED]

    # ── Skill file / content (optional) ──
    skill_file: str = ""
    skill_content: str = ""  # inline skill text (overrides skill_file if set)

    @property
    def needs_github(self) -> bool:
        """Return True if any terminal action requires the GitHub MCP server."""
        return any(
            action in TERMINAL_ACTION_MCP_SERVERS
            for action in self.terminal_actions
        )

    def find(self, store: GraphStore) -> list[Candidate]:
        """Scan the graph for nodes matching this heuristic.

        Default: iterate nodes of ``source_node_type``, evaluate ``metadata_keys``
        with AND/OR logic, emit a Candidate for each match.
        """
        candidates: list[Candidate] = []
        with store.lock:
            for urn in store.nodes_by_type.get(self.source_node_type, []):
                meta = store.G.nodes[urn].get("metadata", {})
                if self.metadata_keys:
                    keys_present = [k for k in self.metadata_keys if k in meta]
                    if self.metadata_key_op == "AND" and len(keys_present) != len(self.metadata_keys):
                        continue
                    if self.metadata_key_op == "OR" and not keys_present:
                        continue
                candidates.append(
                    Candidate(
                        id=candidate_id(urn, self.name),
                        source_urn=urn,
                        source_node_type=self.source_node_type,
                        source_metadata=dict(meta),
                        heuristic_name=self.name,
                        terminal_actions=[str(a) for a in self.terminal_actions],
                        skill_file=self.skill_file,
                    )
                )
        return candidates

    @abstractmethod
    def get_instructions(self) -> str:
        """Return investigation instructions for the agent."""

    def get_playbook(self) -> str | None:
        """Return the skill content, or None if neither skill_content nor skill_file is set."""
        if self.skill_content:
            return self.skill_content
        if not self.skill_file:
            return None
        path = SKILL_DIR / self.skill_file
        if path.exists():
            return path.read_text()
        return None
