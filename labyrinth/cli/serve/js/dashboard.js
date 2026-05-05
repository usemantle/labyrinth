/**
 * Agent Actions dashboard — renders run history, actions timeline, and link evaluations.
 */
import state from "./state.js";

function escapeHtml(str) {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}

function formatTimestamp(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  return d.toLocaleString(undefined, {
    month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
  });
}

function formatTime(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  return d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function urnShort(urn) {
  if (!urn) return "";
  const parts = urn.split(":");
  return parts.length > 3 ? parts.slice(-2).join(":") : urn;
}

// ── Init ──

export async function initDashboard() {
  try {
    const resp = await fetch("reports.json");
    if (!resp.ok) {
      console.warn("[labyrinth] reports.json not found");
      return;
    }
    const data = await resp.json();
    state.reports = data;

    const runs = data.runs || [];
    renderRunList(runs);
    if (runs.length > 0) {
      selectRun(runs[runs.length - 1]);
    }
  } catch (err) {
    console.error("[labyrinth] dashboard init error:", err);
  }
}

// ── Run list sidebar ──

function renderRunList(runs) {
  const container = document.getElementById("run-list");
  if (runs.length === 0) {
    container.innerHTML = '<div style="font-size:12px;color:#6c7086">No runs yet</div>';
    return;
  }

  // Show newest first in sidebar
  const sorted = [...runs].reverse();
  container.innerHTML = sorted.map((run) => {
    const date = formatTimestamp(run.started_at);
    const s = run.summary || {};
    const badges = (run.heuristics_run || [])
      .map((h) => `<span class="heuristic-badge">${escapeHtml(h)}</span>`)
      .join("");
    const counts = [
      s.linked ? `${s.linked} linked` : null,
      s.rejected ? `${s.rejected} rejected` : null,
      s.errors ? `${s.errors} errors` : null,
    ].filter(Boolean).join(", ");

    return `
      <div class="run-item" data-run-id="${escapeHtml(run.run_id)}">
        <div class="run-date">${escapeHtml(date)}</div>
        <div class="run-meta">${escapeHtml(counts)} &middot; ${s.total_candidates || 0} candidates</div>
        <div class="run-badges">${badges}</div>
      </div>
    `;
  }).join("");

  // Click handlers
  container.querySelectorAll(".run-item").forEach((el) => {
    el.addEventListener("click", () => {
      const runId = el.dataset.runId;
      const run = runs.find((r) => r.run_id === runId);
      if (run) selectRun(run);
    });
  });
}

// ── Select a run ──

function selectRun(run) {
  // Highlight in sidebar
  document.querySelectorAll(".run-item").forEach((el) => {
    el.classList.toggle("selected", el.dataset.runId === run.run_id);
  });

  const main = document.getElementById("dashboard-main");
  const empty = document.getElementById("dashboard-empty");
  if (empty) empty.style.display = "none";

  let html = "";
  html += renderRunSummary(run);
  html += renderActionsTimeline(run);
  html += renderLinksSection(run);
  main.innerHTML = html;

  // Wire up collapsible sections
  main.querySelectorAll(".candidate-header").forEach((hdr) => {
    hdr.addEventListener("click", () => {
      hdr.parentElement.classList.toggle("open");
    });
  });
}

// ── Summary cards ──

function renderRunSummary(run) {
  const s = run.summary || {};
  const cards = [
    { value: s.total_candidates || 0, label: "Total", color: "#cdd6f4" },
    { value: s.linked || 0, label: "Linked", color: "#a6e3a1" },
    { value: s.rejected || 0, label: "Rejected", color: "#f9e2af" },
    { value: s.errors || 0, label: "Errors", color: "#f38ba8" },
  ];

  return `
    <div class="summary-cards">
      ${cards.map((c) => `
        <div class="summary-card">
          <div class="card-value" style="color:${c.color}">${c.value}</div>
          <div class="card-label">${c.label}</div>
        </div>
      `).join("")}
    </div>
  `;
}

// ── Actions timeline ──

function renderActionsTimeline(run) {
  const results = run.results || [];
  if (results.length === 0) return '<div style="color:#6c7086;font-size:12px">No results</div>';

  return results.map((r) => {
    const actions = (r.actions || []).map((a) => `
      <div class="action-item">
        <div class="action-time">${escapeHtml(formatTime(a.timestamp))}</div>
        <div class="action-details">
          <span class="action-badge ${escapeHtml(a.action_type)}">${escapeHtml(a.action_type)}</span>
          <div class="action-tool">${escapeHtml(a.tool_name)}</div>
          <div class="action-io">${escapeHtml(JSON.stringify(a.input, null, 2))}</div>
          ${a.output_summary ? `<div class="action-io" style="color:#a6e3a1">${escapeHtml(a.output_summary)}</div>` : ""}
        </div>
      </div>
    `).join("");

    const summary = r.agent_summary
      ? `<div class="agent-summary">${escapeHtml(r.agent_summary)}</div>`
      : "";

    return `
      <div class="candidate-section">
        <div class="candidate-header">
          <span class="caret">&#9654;</span>
          <span class="candidate-urn">${escapeHtml(urnShort(r.candidate_urn))}</span>
          <span class="heuristic-badge">${escapeHtml(r.heuristic_name || "")}</span>
          <span class="outcome-badge ${escapeHtml(r.outcome || "")}">${escapeHtml(r.outcome || "")}</span>
        </div>
        <div class="candidate-body">
          ${summary}
          <div class="actions-timeline">
            <h3>Actions (${(r.actions || []).length})</h3>
            ${actions || '<div style="color:#6c7086;font-size:12px">No MCP actions recorded</div>'}
          </div>
        </div>
      </div>
    `;
  }).join("");
}

// ── Links evaluated section ──

function renderLinksSection(run) {
  const results = (run.results || []).filter((r) => r.links_evaluated && r.links_evaluated.length > 0);
  if (results.length === 0) return "";

  const links = results.flatMap((r) => r.links_evaluated.map((l) => ({ ...l, candidate: r.candidate_urn })));

  return `
    <div class="links-section" style="margin-top:20px">
      <h3>Links Evaluated (${links.length})</h3>
      ${links.map((l) => `
        <div class="link-item">
          <span class="link-urn">${escapeHtml(urnShort(l.from_urn))}</span>
          <span class="link-arrow">&rarr;</span>
          <span class="link-urn">${escapeHtml(urnShort(l.to_urn))}</span>
          <div class="link-meta">
            <span class="heuristic-badge">${escapeHtml(l.edge_type || "")}</span>
            ${l.confidence ? `<span class="confidence-pill ${escapeHtml(l.confidence)}">${escapeHtml(l.confidence)}</span>` : ""}
          </div>
          ${l.rationale ? `<div class="link-rationale">${escapeHtml(l.rationale)}</div>` : ""}
        </div>
      `).join("")}
    </div>
  `;
}
