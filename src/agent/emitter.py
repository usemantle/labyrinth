"""Emit stage: invoke Claude Agent SDK to investigate candidates."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, query

from src.agent.candidates import Candidate, CandidateResult
from src.agent.heuristics._base import OutputType
from src.agent.prompts import build_investigation_prompt, build_system_prompt
from src.mcp.graph_store import GraphStore

logger = logging.getLogger(__name__)


async def emit_candidate(
    candidate: Candidate,
    store: GraphStore,
    project_dir: Path,
) -> CandidateResult:
    """Invoke a Claude agent to investigate a single candidate."""
    prompt = build_investigation_prompt(candidate, store, project_dir)
    system = build_system_prompt()

    agent_result_text = ""

    graph_path = str(project_dir / "graph.json")

    mcp_servers = {
        "knowledge": {
            "command": "uv",
            "args": ["run", "labyrinth", "mcp", "--graph-path", graph_path],
        },
    }
    if candidate.output_type == OutputType.REMEDIATION:
        gat = os.environ.get("GITHUB_ACCESS_TOKEN", None)
        if gat:
            mcp_servers["github"] = {
                "type": "http",
                "url": "https://api.githubcopilot.com/mcp/",
                "headers": {
                    "Authorization": f"Bearer {gat}",
                },
            }

    try:
        async for message in query(
            prompt=prompt,
            options=ClaudeAgentOptions(
                cwd=str(project_dir),
                allowed_tools=["Read", "Grep", "Glob", "Bash"],
                mcp_servers=mcp_servers,
                permission_mode="bypassPermissions",
                system_prompt=system,
                model="claude-sonnet-4-6",
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
        return CandidateResult(
            candidate=candidate,
            outcome="error",
            soft_link_id=None,
            note=f"Agent raised an exception for {candidate.source_urn}",
        )

    # Detect outcome by checking if the agent set the evaluation metadata
    store.reload()
    meta = store.G.nodes[candidate.source_urn].get("metadata", {})
    eval_key = f"{candidate.heuristic_name}_last_evaluated_at"
    if eval_key in meta:
        outcome = "linked"
    else:
        outcome = "rejected"

    return CandidateResult(
        candidate=candidate,
        outcome=outcome,
        soft_link_id=None,
        note=agent_result_text or "Agent did not produce output.",
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
