"""Heuristic: Dockerfiles with ENTRYPOINT/CMD but no `executes` edge to a code file."""

from __future__ import annotations

from labyrinth.agent.candidates import Candidate, candidate_id
from labyrinth.agent.heuristics._base import BaseHeuristic, TerminalAction
from labyrinth.graph.graph_models import NodeMetadataKey, NodeType
from labyrinth.mcp.graph_store import GraphStore


class UnlinkedEntrypoint(BaseHeuristic):
    name = "unlinked_entrypoint"
    source_node_type = NodeType.FILE
    metadata_keys = {}  # Custom find() logic
    terminal_actions = [TerminalAction.MARK_EVALUATED, TerminalAction.CREATE_SOFT_LINK]
    skill_file = "link-dockerfile-to-entrypoint.md"

    def find(self, store: GraphStore) -> list[Candidate]:
        """Return Dockerfiles that have entrypoint metadata but no outgoing executes edge."""
        candidates: list[Candidate] = []
        with store.lock:
            for urn in store.nodes_by_type.get(self.source_node_type, []):
                meta = store.G.nodes[urn].get("metadata", {})
                has_entrypoint = NodeMetadataKey.DOCKERFILE_ENTRYPOINT in meta
                has_cmd = NodeMetadataKey.DOCKERFILE_CMD in meta
                if not has_entrypoint and not has_cmd:
                    continue

                # Check for existing outgoing executes edge
                has_executes = False
                for _, _, edge_data in store.G.out_edges(urn, data=True):
                    if edge_data.get("edge_type") == "executes":
                        has_executes = True
                        break

                if has_executes:
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

    def get_instructions(self) -> str:
        return (
            "This Dockerfile has an ENTRYPOINT or CMD instruction but no outgoing `executes` "
            "edge to a code file. Investigate which source file is the container entrypoint "
            "and create a soft link if evidence supports a match. Follow the investigation "
            "steps in the skill file."
        )
