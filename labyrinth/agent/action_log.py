"""Action capture for agent MCP tool calls."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from claude_agent_sdk import HookContext, HookJSONOutput, HookMatcher, PostToolUseHookInput

# ── Action type classification ────────────────────────────────────────

_TOOL_ACTION_MAP: dict[str, str] = {
    "mcp__knowledge__add_soft_link": "SOFT_LINK_ADDED",
    "mcp__knowledge__remove_soft_link": "SOFT_LINK_REMOVED",
    "mcp__github__create_pull_request": "PR_CREATED",
    "mcp__github__create_branch": "BRANCH_CREATED",
    "mcp__github__create_or_update_file": "FILE_CHANGED",
}


def classify_action(tool_name: str, tool_input: dict[str, Any]) -> str:
    """Map a tool name (+input) to a human-readable action type."""
    if tool_name in _TOOL_ACTION_MAP:
        return _TOOL_ACTION_MAP[tool_name]

    if tool_name == "mcp__knowledge__update_node_metadata":
        updates = tool_input.get("updates", {})
        if any(k.endswith("_last_evaluated_at") for k in updates):
            return "EVALUATED_AT_MARKED"
        return "METADATA_UPDATED"

    if tool_name.startswith("mcp__"):
        return "MCP_TOOL_CALL"

    return "UNKNOWN"


# ── Data model ────────────────────────────────────────────────────────

@dataclass
class CapturedAction:
    """A single MCP tool call captured by the agent."""

    timestamp: str
    action_type: str
    tool_name: str
    server: str
    input: dict[str, Any]
    output_summary: str


def _extract_server(tool_name: str) -> str:
    """Extract server name from tool name like mcp__knowledge__add_soft_link."""
    parts = tool_name.split("__")
    return parts[1] if len(parts) >= 3 else "unknown"


def _summarize_output(response: Any) -> str:
    """Create a short string summary of a tool response."""
    text = str(response) if response is not None else ""
    if len(text) > 300:
        return text[:300] + "..."
    return text


# ── Collector ─────────────────────────────────────────────────────────

@dataclass
class ActionCollector:
    """Collects CapturedAction items from PostToolUse hooks."""

    actions: list[CapturedAction] = field(default_factory=list)

    def record(self, tool_name: str, tool_input: dict[str, Any], tool_response: Any) -> None:
        """Record an MCP tool call if it matches the mcp__ prefix."""
        if not tool_name.startswith("mcp__"):
            return

        action = CapturedAction(
            timestamp=datetime.now(UTC).isoformat(),
            action_type=classify_action(tool_name, tool_input),
            tool_name=tool_name,
            server=_extract_server(tool_name),
            input=dict(tool_input),
            output_summary=_summarize_output(tool_response),
        )
        self.actions.append(action)

    def as_hook(self) -> HookMatcher:
        """Return a HookMatcher suitable for ClaudeAgentOptions.hooks['PostToolUse']."""

        async def _hook(
            hook_input: PostToolUseHookInput | dict,
            tool_use_id: str | None,
            context: HookContext,
        ) -> HookJSONOutput:
            # SDK may pass a raw dict rather than a typed object
            if isinstance(hook_input, dict):
                tool_name = hook_input.get("tool_name", "")
                tool_input = hook_input.get("tool_input", {})
                tool_response = hook_input.get("tool_response")
            else:
                tool_name = hook_input.tool_name
                tool_input = hook_input.tool_input
                tool_response = hook_input.tool_response
            self.record(tool_name, tool_input, tool_response)
            return {}

        return HookMatcher(matcher=None, hooks=[_hook])

    def extract_soft_link_id(self) -> str | None:
        """Extract the soft link ID from the first SOFT_LINK_ADDED action output."""
        for action in self.actions:
            if action.action_type == "SOFT_LINK_ADDED":
                match = re.search(r"id[=:][\s]*([a-f0-9-]+)", action.output_summary, re.IGNORECASE)
                if match:
                    return match.group(1)
        return None

    def extract_links_evaluated(self) -> list[dict]:
        """Extract link info from SOFT_LINK_ADDED action inputs."""
        links: list[dict] = []
        for action in self.actions:
            if action.action_type == "SOFT_LINK_ADDED":
                links.append({
                    "from_urn": action.input.get("from_urn", ""),
                    "to_urn": action.input.get("to_urn", ""),
                    "edge_type": action.input.get("edge_type", ""),
                    "confidence": action.input.get("confidence", ""),
                    "rationale": action.input.get("note", ""),
                })
        return links
