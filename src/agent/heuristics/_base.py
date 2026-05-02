"""Base class for all heuristics."""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal

import networkx as nx

from src.agent.candidates import Candidate, candidate_id
from src.mcp.graph_store import GraphStore

# A metadata filter dict.
#   {"http_method": True}            -> key must be present
#   {"auth_scheme": "none"}          -> key present AND value == "none"
#   {"http_method": True, "x": "y"}  -> combined under metadata_key_op (AND/OR)
MetadataFilter = dict[str, "bool | str"]

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

    Subclasses define what to look for (``source_node_type`` + ``metadata_keys``),
    the terminal actions, and how to investigate (instructions + optional playbook).

    Two modes are supported by the default ``find()``:

    * **Source-only** (``dest_node_type`` is ``None``): iterate all nodes of
      ``source_node_type``, apply the metadata-filter dict with AND/OR
      combination, emit a ``Candidate`` per match. This is the original
      contract.

    * **Path-linked** (``dest_node_type`` is set): also collect nodes of
      ``dest_node_type`` filtered by ``dest_node_metadata``, then for every
      (source, dest) pair compute an undirected shortest path on the graph.
      One ``Candidate`` per pair *that has a path* is emitted, carrying
      ``dest_urn``, ``dest_node_type``, ``dest_metadata``, and ``path``
      (the URN sequence). Pairs with no path are dropped.

    Subclasses with bespoke matching can still override ``find()``.
    """

    # ── Identity ──
    name: str  # unique key, e.g. "unlinked_dockerfile"

    # ── Source side ──
    source_node_type: str  # e.g. "file", "function", "class"
    metadata_keys: MetadataFilter = {}  # presence (True) or value (str) per key
    metadata_key_op: Literal["AND", "OR"] = "OR"

    # ── Optional dest side (path-linked mode) ──
    dest_node_type: str | None = None
    dest_node_metadata: MetadataFilter = {}
    dest_metadata_key_op: Literal["AND", "OR"] = "OR"

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

    # ── Matching helpers ──

    @staticmethod
    def _matches_filter(
        meta: dict,
        filters: MetadataFilter,
        key_op: Literal["AND", "OR"],
    ) -> bool:
        """Return True if ``meta`` satisfies the filter dict under AND/OR.

        An empty filter dict always passes.
        ``True``-valued filter: key must be present in ``meta``.
        ``str``-valued filter: key must be present AND ``meta[key] == value``.
        """
        if not filters:
            return True

        def _hit(key: str, expected) -> bool:
            if key not in meta:
                return False
            # ``is True`` is a deliberate identity check, not a truthiness test:
            # only the literal boolean True selects presence-only mode. Strings
            # (including non-empty ones like "ACTIVE" or even "True") fall
            # through to the value-equality branch below.
            if expected is True:
                return True
            return meta[key] == expected

        if key_op == "AND":
            return all(_hit(k, v) for k, v in filters.items())
        # OR (default)
        return any(_hit(k, v) for k, v in filters.items())

    def _find_candidates(
        self,
        store: GraphStore,
        node_type: str,
        filters: MetadataFilter,
        key_op: Literal["AND", "OR"],
    ) -> list[tuple[str, dict]]:
        """Return ``(urn, metadata)`` tuples for nodes of ``node_type`` that match the filter."""
        out: list[tuple[str, dict]] = []
        for urn in store.nodes_by_type.get(node_type, []):
            meta = store.G.nodes[urn].get("metadata", {})
            if self._matches_filter(meta, filters, key_op):
                out.append((urn, dict(meta)))
        return out

    # ── Public API ──

    def find(self, store: GraphStore) -> list[Candidate]:
        """Scan the graph for candidate nodes (and optionally paths) matching this heuristic."""
        candidates: list[Candidate] = []
        with store.lock:
            sources = self._find_candidates(
                store, self.source_node_type, self.metadata_keys, self.metadata_key_op,
            )
            if not sources:
                return candidates

            if not self.dest_node_type:
                for urn, meta in sources:
                    candidates.append(self._build_source_candidate(urn, meta))
                return candidates

            dests = self._find_candidates(
                store,
                self.dest_node_type,
                self.dest_node_metadata,
                self.dest_metadata_key_op,
            )
            if not dests:
                return candidates

            undirected = store.G.to_undirected(as_view=False)
            for src_urn, src_meta in sources:
                for dst_urn, dst_meta in dests:
                    if src_urn == dst_urn:
                        continue
                    try:
                        path = nx.shortest_path(undirected, src_urn, dst_urn)
                    except nx.NetworkXNoPath:
                        continue
                    except nx.NodeNotFound:
                        continue
                    candidates.append(self._build_linked_candidate(
                        src_urn, src_meta, dst_urn, dst_meta, path,
                    ))
        return candidates

    def _build_source_candidate(self, urn: str, meta: dict) -> Candidate:
        return Candidate(
            id=candidate_id(urn, self.name),
            source_urn=urn,
            source_node_type=self.source_node_type,
            source_metadata=meta,
            heuristic_name=self.name,
            terminal_actions=[str(a) for a in self.terminal_actions],
            skill_file=self.skill_file,
        )

    def _build_linked_candidate(
        self,
        src_urn: str,
        src_meta: dict,
        dst_urn: str,
        dst_meta: dict,
        path: list[str],
    ) -> Candidate:
        return Candidate(
            id=candidate_id(src_urn, self.name, dest_urn=dst_urn),
            source_urn=src_urn,
            source_node_type=self.source_node_type,
            source_metadata=src_meta,
            heuristic_name=self.name,
            terminal_actions=[str(a) for a in self.terminal_actions],
            skill_file=self.skill_file,
            dest_urn=dst_urn,
            dest_node_type=self.dest_node_type,
            dest_metadata=dst_meta,
            path=path,
        )

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
