"""Emit stage: invoke Claude Agent SDK to investigate candidates."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, query

from src.agent.action_log import ActionCollector
from src.agent.candidates import Candidate, CandidateResult
from src.agent.heuristics._base import TERMINAL_ACTION_MCP_SERVERS, TerminalAction
from src.agent.prompts import build_investigation_prompt, build_system_prompt
from src.agent.worktree import cleanup_worktree, create_worktree, resolve_repo_root, worktree_has_changes
from src.mcp.graph_store import GraphStore

logger = logging.getLogger(__name__)


def _needs_worktree(candidate: Candidate) -> bool:
    """Return True if the candidate's terminal actions may modify the filesystem."""
    return str(TerminalAction.CREATE_PR) in candidate.terminal_actions


def _collect_mcp_servers(candidate: Candidate, graph_path: str) -> dict:
    """Build the MCP server dict from the candidate's terminal actions."""
    servers: dict = {
        "knowledge": {
            "command": "uv",
            "args": ["run", "labyrinth", "mcp", "--graph-path", graph_path],
        },
    }
    for action_str in candidate.terminal_actions:
        action = TerminalAction(action_str)
        if action in TERMINAL_ACTION_MCP_SERVERS:
            for name, config in TERMINAL_ACTION_MCP_SERVERS[action].items():
                if name == "github" and not os.environ.get("GITHUB_PERSONAL_ACCESS_TOKEN"):
                    continue
                servers[name] = config
    return servers


async def _generate_summary(agent_result_text: str, collector: ActionCollector) -> str:
    """Generate a short summary of the agent's investigation using a lightweight model."""
    action_descriptions = [
        f"- {a.action_type}: {a.tool_name}({', '.join(f'{k}={v!r}' for k, v in list(a.input.items())[:3])})"
        for a in collector.actions
    ]
    actions_text = "\n".join(action_descriptions) if action_descriptions else "(no MCP actions taken)"

    prompt = (
        "Summarize what you investigated and why in 2-4 sentences.\n\n"
        f"## Agent result\n{agent_result_text[:2000]}\n\n"
        f"## Actions taken\n{actions_text}"
    )

    summary = ""
    try:
        async for message in query(
            prompt=prompt,
            options=ClaudeAgentOptions(
                model="claude-haiku-4-5",
                permission_mode="bypassPermissions",
                system_prompt="You are a concise summarizer. Output only the summary, nothing else.",
            ),
        ):
            if isinstance(message, ResultMessage):
                summary = message.result or ""
    except Exception:
        logger.exception("Failed to generate agent summary")

    return summary


async def emit_candidate(
    candidate: Candidate,
    store: GraphStore,
    project_dir: Path,
) -> CandidateResult:
    """Invoke a Claude agent to investigate a single candidate."""
    system = build_system_prompt()

    agent_result_text = ""
    collector = ActionCollector()

    graph_path = str(project_dir / "graph.json")
    mcp_servers = _collect_mcp_servers(candidate, graph_path)

    # Create a worktree if the candidate may modify files
    worktree_path: Path | None = None
    worktree_branch: str | None = None
    repo_root: Path | None = None

    if _needs_worktree(candidate):
        repo_root = resolve_repo_root(candidate.source_urn, project_dir)
        if repo_root:
            worktree_dir = project_dir / ".worktrees"
            try:
                worktree_path, worktree_branch = create_worktree(repo_root, worktree_dir)
                logger.info(
                    "Agent will operate in worktree %s (branch %s)",
                    worktree_path, worktree_branch,
                )
            except Exception:
                logger.exception("Failed to create worktree for %s", repo_root)
                worktree_path = None
                worktree_branch = None
        else:
            logger.warning(
                "Could not resolve git repo root for %s; agent will use primary worktree",
                candidate.source_urn,
            )

    prompt = build_investigation_prompt(
        candidate, store, project_dir,
        worktree_path=str(worktree_path) if worktree_path else None,
        original_repo_root=str(repo_root) if repo_root else None,
    )

    # When a worktree is active, set cwd to the worktree so the agent
    # operates there by default instead of the original repo.
    agent_cwd = str(worktree_path) if worktree_path else str(project_dir)

    try:
        async for message in query(
            prompt=prompt,
            options=ClaudeAgentOptions(
                cwd=agent_cwd,
                allowed_tools=["Read", "Grep", "Glob", "Bash"],
                mcp_servers=mcp_servers,
                permission_mode="bypassPermissions",
                system_prompt=system,
                model="claude-sonnet-4-6",
                hooks={"PostToolUse": [collector.as_hook()]},
            ),
        ):
            if isinstance(message, ResultMessage):
                agent_result_text = message.result or ""
                logger.info(
                    "Agent finished for %s: %s",
                    candidate.source_urn,
                    agent_result_text[:200] if agent_result_text else "(no result)",
                )
    except Exception:
        logger.exception("Agent error for %s", candidate.source_urn)
        # Clean up worktree on error
        if worktree_path and repo_root and worktree_branch:
            cleanup_worktree(repo_root, worktree_path, worktree_branch)
        return CandidateResult(
            candidate=candidate,
            outcome="error",
            note=f"Agent raised an exception for {candidate.source_urn}",
            actions=collector.actions,
        )

    # Detect outcome by checking if the agent set the evaluation metadata
    store.reload()
    meta = store.G.nodes[candidate.source_urn].get("metadata", {})
    eval_key = f"{candidate.heuristic_name}_last_evaluated_at"
    if eval_key in meta:
        outcome = "linked"
    else:
        outcome = "rejected"

    # Clean up worktree if no changes were made
    wt_path_str: str | None = None
    wt_branch_str: str | None = None
    if worktree_path and repo_root and worktree_branch:
        if worktree_has_changes(worktree_path):
            wt_path_str = str(worktree_path)
            wt_branch_str = worktree_branch
            logger.info("Worktree %s has changes, keeping for review", worktree_path)
        else:
            cleanup_worktree(repo_root, worktree_path, worktree_branch)
            logger.info("Worktree had no changes, cleaned up")

    # Generate a summary of the investigation
    agent_summary = await _generate_summary(agent_result_text, collector)

    return CandidateResult(
        candidate=candidate,
        outcome=outcome,
        note=agent_result_text or "Agent did not produce output.",
        actions=collector.actions,
        agent_summary=agent_summary,
        soft_link_id=collector.extract_soft_link_id(),
        links_evaluated=collector.extract_links_evaluated() or None,
        worktree_path=wt_path_str,
        worktree_branch=wt_branch_str,
    )


async def emit_all(
    candidates: list[Candidate],
    store: GraphStore,
    project_dir: Path,
) -> list[CandidateResult]:
    """Investigate all candidates sequentially."""
    results: list[CandidateResult] = []
    for i, candidate in enumerate(candidates, 1):
        logger.info(
            "Investigating candidate %d/%d: %s (%s)",
            i,
            len(candidates),
            candidate.source_urn,
            candidate.heuristic_name,
        )
        result = await emit_candidate(candidate, store, project_dir)
        results.append(result)
        logger.info("  → %s", result.outcome)
    return results
