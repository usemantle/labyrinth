"""Prompt construction for the discovery agent."""

from __future__ import annotations

import logging
from pathlib import Path

from src.agent.candidates import Candidate
from src.agent.heuristics import HEURISTICS_BY_NAME
from src.agent.heuristics._base import TERMINAL_ACTION_PROMPTS, TerminalAction
from src.mcp.graph_store import GraphStore

logger = logging.getLogger(__name__)

MAX_TARGET_NODES = 20


def build_system_prompt() -> str:
    """Build the system prompt for the discovery agent."""
    return (
        "You are a security graph analyst. Your job is to investigate candidate "
        "nodes in a knowledge graph and take the post-investigation actions "
        "specified in the investigation prompt.\n\n"
        "## Confidence guidelines (for soft link actions)\n\n"
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
        "- Always include detailed notes explaining your evidence.\n"
        "- Use the MCP knowledge tools (search_nodes, get_neighbors, get_node_details) "
        "to investigate.\n"
        "- Use Read, Grep, and Glob to examine source code for additional evidence.\n"
        "- Follow all post-investigation actions listed in the investigation prompt.\n"
    )


def build_investigation_prompt(
    candidate: Candidate,
    store: GraphStore,
    project_dir: Path | None = None,
    *,
    worktree_path: str | None = None,
    original_repo_root: str | None = None,
) -> str:
    """Construct a per-candidate investigation prompt."""
    parts: list[str] = []

    # 1. Candidate context
    parts.append("## Candidate\n")
    parts.append(f"- **URN:** `{candidate.source_urn}`")
    parts.append(f"- **Node type:** {candidate.source_node_type}")
    parts.append(f"- **Heuristic:** {candidate.heuristic_name}")
    parts.append(f"- **Actions:** {', '.join(candidate.terminal_actions)}")
    parts.append(f"\n### Metadata\n```\n{candidate.source_metadata}\n```\n")

    # 2. Worktree isolation instructions
    if worktree_path and original_repo_root:
        parts.append("## Worktree isolation\n")
        parts.append(
            "A git worktree has been created for this investigation. "
            "**All file modifications MUST be made in the worktree, not the original repository.**\n"
        )
        parts.append(f"- **Original repo:** `{original_repo_root}`")
        parts.append(f"- **Worktree path:** `{worktree_path}`")
        parts.append(
            "\nYou may read files from the original repo for investigation, but any edits, "
            "commits, or branch operations must happen in the worktree path. "
            "When using Bash to run git commands or edit files, always `cd` to the worktree first.\n"
        )

    # 3. Task description — pulled from the heuristic class
    heuristic = HEURISTICS_BY_NAME.get(candidate.heuristic_name)
    instruction = (
        heuristic.get_instructions()
        if heuristic
        else "Investigate this candidate and determine if action is needed."
    )
    parts.append(f"## Task\n\n{instruction}\n")

    # 4. Skill content (if available)
    if heuristic:
        playbook = heuristic.get_playbook()
        if playbook:
            parts.append(f"## Investigation playbook\n\n{playbook}\n")

    # 5. Available nodes for soft link heuristics
    actions = [TerminalAction(a) for a in candidate.terminal_actions]
    if TerminalAction.CREATE_SOFT_LINK in actions:
        with store.lock:
            target_urns = store.nodes_by_type.get(candidate.source_node_type, [])[:MAX_TARGET_NODES]
        if target_urns:
            parts.append(f"## Available nodes ({len(target_urns)} shown)\n")
            for urn in target_urns:
                parts.append(f"- `{urn}`")
            parts.append("")

    # 6. Post-investigation actions — assembled from terminal actions
    parts.append("## Post-investigation actions\n")
    parts.append(
        "Use the MCP knowledge tools and codebase tools to investigate. "
        "When your investigation is complete, perform the following actions in order:\n"
    )
    for i, action in enumerate(actions, 1):
        template = TERMINAL_ACTION_PROMPTS[action]
        text = template.format(
            source_urn=candidate.source_urn,
            heuristic_name=candidate.heuristic_name,
        )
        parts.append(f"{i}. {text}\n")

    return "\n".join(parts)
