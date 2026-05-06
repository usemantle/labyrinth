"""Heuristic: HTTP endpoints without authentication."""

from __future__ import annotations

from labyrinth.agent.candidates import Candidate, candidate_id
from labyrinth.agent.heuristics._base import BaseHeuristic, TerminalAction
from labyrinth.graph.graph_models import NodeMetadataKey, NodeType
from labyrinth.mcp.graph_store import GraphStore


class InsecureEndpoint(BaseHeuristic):
    name = "insecure_endpoint"
    source_node_type = NodeType.FUNCTION
    metadata_keys = {NodeMetadataKey.HTTP_METHOD: True}
    terminal_actions = [TerminalAction.MARK_EVALUATED, TerminalAction.CREATE_PR]
    skill_file = "detect-insecure-endpoint.md"

    def find(self, store: GraphStore) -> list[Candidate]:
        """Find HTTP endpoints that have no auth_scheme metadata."""
        candidates: list[Candidate] = []
        with store.lock:
            for fn_urn in store.nodes_by_type.get(NodeType.FUNCTION, []):
                meta = store.G.nodes[fn_urn].get("metadata", {})
                if NodeMetadataKey.HTTP_METHOD not in meta:
                    continue
                if NodeMetadataKey.AUTH_SCHEME in meta:
                    continue
                candidates.append(
                    Candidate(
                        id=candidate_id(fn_urn, self.name),
                        source_urn=fn_urn,
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
            "This HTTP endpoint has no detected authentication scheme. Investigate "
            "whether it handles unsanitized user input, is vulnerable to path traversal "
            "or IDOR attacks, and whether it is deployed to an internet-facing service. "
            "Follow the skill file for investigation steps."
        )
