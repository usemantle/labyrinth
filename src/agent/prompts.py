"""Prompt construction for the soft-link discovery agent."""

from __future__ import annotations

import logging
from pathlib import Path

from src.agent.candidates import Candidate
from src.agent.heuristics import HEURISTICS_BY_NAME
from src.mcp.graph_store import GraphStore

logger = logging.getLogger(__name__)

MAX_TARGET_NODES = 20


def build_system_prompt() -> str:
    """Build the system prompt for the discovery agent."""
    return (
        "You are a security graph analyst. Your job is to investigate candidate "
        "relationships in a knowledge graph and create soft links when evidence "
        "supports them.\n\n"
        "## Confidence guidelines\n\n"
        "| Level | When to use |\n"
        "|-------|-------------|\n"
        "| VERY_HIGH | Hard evidence: IaC defines the resource, OCI labels match, "
        "bucket name is hardcoded |\n"
        "| HIGH | Strong circumstantial: naming matches, only one candidate, "
        "ECS confirms the relationship |\n"
        "| MEDIUM | Reasonable inference: naming patterns suggest a match, "
        "but multiple candidates exist |\n"
        "| LOW | Weak evidence: only loose naming similarity, no corroborating signals |\n\n"
        "## Rules\n"
        "- Only call add_soft_link when you have sufficient evidence.\n"
        "- Always include a detailed note explaining your evidence.\n"
        "- If you cannot find sufficient evidence, explain why and do NOT create a link.\n"
        "- Use the MCP knowledge tools (search_nodes, get_neighbors, get_node_details) "
        "to investigate.\n"
        "- Use Read, Grep, and Glob to examine source code for additional evidence.\n"
    )


def build_investigation_prompt(candidate: Candidate, store: GraphStore, project_dir: Path | None = None) -> str:
    """Construct a per-candidate investigation prompt."""
    parts: list[str] = []

    # 1. Candidate context
    parts.append("## Candidate\n")
    parts.append(f"- **URN:** `{candidate.source_urn}`")
    parts.append(f"- **Node type:** {candidate.source_node_type}")
    parts.append(f"- **Heuristic:** {candidate.heuristic_name}")
    parts.append(f"- **Target edge type:** {candidate.target_edge_type}")
    parts.append(f"- **Target node type:** {candidate.target_node_type}")
    parts.append(f"\n### Metadata\n```\n{candidate.source_metadata}\n```\n")

    # 2. Task description — pulled from the heuristic class
    heuristic = HEURISTICS_BY_NAME.get(candidate.heuristic_name)
    instruction = (
        heuristic.get_instructions()
        if heuristic
        else "Investigate this candidate and determine if a soft link should be created."
    )
    parts.append(f"## Task\n\n{instruction}\n")

    # 3. Skill content (if available)
    if heuristic:
        playbook = heuristic.get_playbook()
        if playbook:
            parts.append(f"## Investigation playbook\n\n{playbook}\n")

    # 4. Available targets
    with store.lock:
        target_urns = store.nodes_by_type.get(candidate.target_node_type, [])[:MAX_TARGET_NODES]
    if target_urns:
        parts.append(f"## Available {candidate.target_node_type} nodes ({len(target_urns)} shown)\n")
        for urn in target_urns:
            parts.append(f"- `{urn}`")
        parts.append("")
    else:
        parts.append(f"## Available {candidate.target_node_type} nodes\n\nNone found in graph.\n")

    # 5. Output instructions
    parts.append(
        "## Instructions\n\n"
        "Use the MCP knowledge tools and codebase tools to investigate. "
        "If evidence is sufficient, call `add_soft_link` with appropriate "
        "edge_type, confidence, and note. If evidence is insufficient, "
        "explain your reasoning.\n"
    )

    return "\n".join(parts)
