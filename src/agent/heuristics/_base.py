"""Base class for all heuristics."""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from pathlib import Path

from src.agent.candidates import Candidate
from src.mcp.graph_store import GraphStore

SKILL_DIR = Path(__file__).resolve().parent.parent / "skills"


class OutputType(enum.StrEnum):
    SOFT_LINK = "soft_link"       # Agent creates a soft link between nodes
    REMEDIATION = "remediation"   # Agent evaluates risk, marks findings, creates PR


class BaseHeuristic(ABC):
    """A heuristic detects candidate nodes worth investigating.

    Subclasses define what to look for (source node type, metadata key),
    the output type, and how to investigate (instructions + optional playbook).

    The default ``find()`` implementation covers the common pattern:
    iterate all nodes of ``source_node_type``, check for ``metadata_key``
    presence, and emit a ``Candidate`` for each match.  Override ``find()``
    for heuristics that need custom logic.
    """

    # ── Identity ──
    name: str  # unique key, e.g. "unlinked_dockerfile"

    # ── What to search ──
    source_node_type: str  # e.g. "file", "function", "class"
    metadata_key: str  # metadata key whose presence triggers this heuristic

    # ── Output type ──
    output_type: OutputType = OutputType.SOFT_LINK

    # ── Skill file (optional) ──
    skill_file: str = ""

    def find(self, store: GraphStore) -> list[Candidate]:
        """Scan the graph for nodes matching this heuristic.

        Default: iterate nodes of ``source_node_type``, check for
        ``metadata_key`` in metadata, emit a Candidate for each match.
        """
        candidates: list[Candidate] = []
        with store.lock:
            for urn in store.nodes_by_type.get(self.source_node_type, []):
                meta = store.G.nodes[urn].get("metadata", {})
                if self.metadata_key and self.metadata_key not in meta:
                    continue
                candidates.append(
                    Candidate(
                        source_urn=urn,
                        source_node_type=self.source_node_type,
                        source_metadata=dict(meta),
                        heuristic_name=self.name,
                        output_type=self.output_type,
                        skill_file=self.skill_file,
                    )
                )
        return candidates

    @classmethod
    @abstractmethod
    def get_instructions(cls) -> str:
        """Return investigation instructions for the agent."""

    def get_playbook(self) -> str | None:
        """Return the skill file content, or None if no skill file is set."""
        if not self.skill_file:
            return None
        path = SKILL_DIR / self.skill_file
        if path.exists():
            return path.read_text()
        return None
