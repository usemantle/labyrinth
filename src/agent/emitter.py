"""Emit stage: invoke Claude Agent SDK to investigate candidates."""

from __future__ import annotations

import logging
from pathlib import Path

from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, query

from src.agent.candidates import Candidate, CandidateResult
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

    soft_links_before = len(store.soft_links)
    agent_result_text = ""

    graph_path = str(project_dir / "graph.json")

    try:
        async for message in query(
            prompt=prompt,
            options=ClaudeAgentOptions(
                cwd=str(project_dir),
                allowed_tools=["Read", "Grep", "Glob", "Bash"],
                mcp_servers={
                    "knowledge": {
                        "command": "uv",
                        "args": ["run", "labyrinth", "mcp", "--graph-path", graph_path],
                    },
                },
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

    # Detect outcome by reloading soft links and checking if any were added
    store.reload()
    new_links = store.soft_links[soft_links_before:]
    if new_links:
        link_id = new_links[0].get("id", "unknown")
        return CandidateResult(
            candidate=candidate,
            outcome="linked",
            soft_link_id=link_id,
            note=new_links[0].get("note", ""),
        )

    return CandidateResult(
        candidate=candidate,
        outcome="rejected",
        soft_link_id=None,
        note=agent_result_text or "Agent did not create a soft link (no output).",
    )


async def emit_all(
    candidates: list[Candidate],
    store: GraphStore,
    project_dir: Path,
) -> list[CandidateResult]:
    """Investigate all candidates sequentially.

    Sequential execution because each agent invocation shares soft_links.json.
    """
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
