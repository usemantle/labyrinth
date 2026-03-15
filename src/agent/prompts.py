"""Prompt construction for the discovery agent."""

from __future__ import annotations

import logging
from pathlib import Path

from src.agent.candidates import Candidate
from src.agent.heuristics import HEURISTICS_BY_NAME
from src.agent.heuristics._base import OutputType
from src.mcp.graph_store import GraphStore

logger = logging.getLogger(__name__)

MAX_TARGET_NODES = 20


def build_system_prompt() -> str:
    """Build the system prompt for the discovery agent."""
    return (
        "You are a security graph analyst. Your job is to investigate candidate "
        "nodes in a knowledge graph and take action based on the output type.\n\n"
        "## For SOFT_LINK output type\n\n"
        "Create soft links when evidence supports a relationship between nodes.\n\n"
        "### Confidence guidelines\n\n"
        "| Level | When to use |\n"
        "|-------|-------------|\n"
        "| VERY_HIGH | Hard evidence: IaC defines the resource, OCI labels match, "
        "bucket name is hardcoded |\n"
        "| HIGH | Strong circumstantial: naming matches, only one candidate, "
        "ECS confirms the relationship |\n"
        "| MEDIUM | Reasonable inference: naming patterns suggest a match, "
        "but multiple candidates exist |\n"
        "| LOW | Weak evidence: only loose naming similarity, no corroborating signals |\n\n"
        "## For REMEDIATION output type\n\n"
        "Evaluate risk, document findings, and mark the node as evaluated using "
        "the `update_node_metadata` tool.\n\n"
        "## Rules\n"
        "- For SOFT_LINK: only call add_soft_link when you have sufficient evidence.\n"
        "- For REMEDIATION: always call update_node_metadata to mark evaluation complete.\n"
        "- Always include detailed notes explaining your evidence.\n"
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
    parts.append(f"- **Output type:** {candidate.output_type}")
    parts.append(f"\n### Metadata\n```\n{candidate.source_metadata}\n```\n")

    # 2. Task description — pulled from the heuristic class
    heuristic = HEURISTICS_BY_NAME.get(candidate.heuristic_name)
    instruction = (
        heuristic.get_instructions()
        if heuristic
        else "Investigate this candidate and determine if action is needed."
    )
    parts.append(f"## Task\n\n{instruction}\n")

    # 3. Skill content (if available)
    if heuristic:
        playbook = heuristic.get_playbook()
        if playbook:
            parts.append(f"## Investigation playbook\n\n{playbook}\n")

    # 4. Output-type-specific sections
    if candidate.output_type == OutputType.SOFT_LINK:
        # Show available target nodes for soft link heuristics
        with store.lock:
            target_urns = store.nodes_by_type.get(candidate.source_node_type, [])[:MAX_TARGET_NODES]
        if target_urns:
            parts.append(f"## Available nodes ({len(target_urns)} shown)\n")
            for urn in target_urns:
                parts.append(f"- `{urn}`")
            parts.append("")

        parts.append(
            "## Instructions\n\n"
            "Use the MCP knowledge tools and codebase tools to investigate. "
            "If evidence is sufficient, call `add_soft_link` with appropriate "
            "edge_type, confidence, and note. If evidence is insufficient, "
            "explain your reasoning.\n\n"
            f"After completing your investigation, call `update_node_metadata` with:\n"
            f'- urn: `{candidate.source_urn}`\n'
            f'- metadata: `{{"{candidate.heuristic_name}_last_evaluated_at": "<current ISO timestamp>"}}`\n'
        )
    elif candidate.output_type == OutputType.REMEDIATION:
        parts.append(
            "## Instructions\n\n"
            "Use the MCP knowledge tools, codebase tools, and any available external "
            "tools to evaluate this candidate.\n\n"
            "When your investigation is complete, call `update_node_metadata` with:\n"
            f'- urn: `{candidate.source_urn}`\n'
            f'- metadata: a JSON object including at minimum `"{candidate.heuristic_name}_last_evaluated_at"` '
            f"set to the current ISO timestamp, plus any finding/risk fields documented in the playbook.\n"
        )

    return "\n".join(parts)
