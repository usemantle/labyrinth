/**
 * Node hover tooltip + edge hover tooltip (soft link confidence/evidence).
 */
import state from "./state.js";
import { NODE_STYLES, EDGE_STYLES } from "./styles.js";
import { formatMetadataValue } from "./utils.js";

const CONFIDENCE_COLORS = {
  VERY_HIGH: "#22c55e",
  HIGH: "#84cc16",
  MEDIUM: "#f59e0b",
  LOW: "#ef4444",
};

function buildNodeTooltipHTML(nodeKey) {
  const attrs = state.graph.getNodeAttributes(nodeKey);
  const style = NODE_STYLES[attrs._type] || NODE_STYLES.unknown;
  const meta = attrs._metadata || {};

  let html = `<div class="tt-header"><span class="tt-badge" style="background:${style.color}">${attrs._type}</span></div>`;
  html += `<div class="tt-urn">${nodeKey}</div>`;

  const entries = Object.entries(meta);
  if (entries.length > 0) {
    html += "<table>";
    for (const [k, v] of entries) {
      html += `<tr><td>${k}</td><td>${formatMetadataValue(v)}</td></tr>`;
    }
    html += "</table>";
  }
  return html;
}

function buildEdgeTooltipHTML(edgeKey) {
  const attrs = state.graph.getEdgeAttributes(edgeKey);
  const src = state.graph.source(edgeKey);
  const tgt = state.graph.target(edgeKey);
  const edgeType = attrs._relationType || "unknown";
  const style = EDGE_STYLES[edgeType] || EDGE_STYLES.contains;
  const meta = attrs._metadata || {};
  const isSoftLink = state.softLinkIds.has(attrs._uuid) || meta.detection_method === "soft_link";

  let html = `<div class="tt-header"><span class="tt-badge" style="background:${style.color}">${edgeType}</span></div>`;

  const srcLabel = state.graph.getNodeAttribute(src, "label") || src;
  const tgtLabel = state.graph.getNodeAttribute(tgt, "label") || tgt;
  html += `<div class="tt-urn">${srcLabel} &rarr; ${tgtLabel}</div>`;

  if (isSoftLink && meta.confidence) {
    const confColor = CONFIDENCE_COLORS[meta.confidence] || "#6c7086";
    html += `<div style="padding:4px 12px"><span class="tt-pill" style="background:${confColor}">${meta.confidence}</span></div>`;
  }

  const entries = Object.entries(meta);
  if (entries.length > 0) {
    html += "<table>";
    for (const [k, v] of entries) {
      html += `<tr><td>${k}</td><td>${formatMetadataValue(v)}</td></tr>`;
    }
    html += "</table>";
  }
  return html;
}

export function setupTooltip() {
  const tooltip = document.getElementById("tooltip");
  const container = document.getElementById("sigma-container");

  container.addEventListener("mousemove", (e) => {
    state.mouseX = e.clientX;
    state.mouseY = e.clientY;

    let html = null;
    if (state.hoveredEdge && state.graph) {
      html = buildEdgeTooltipHTML(state.hoveredEdge);
    } else if (state.hoveredNode && state.graph) {
      html = buildNodeTooltipHTML(state.hoveredNode);
    }

    if (!html) {
      tooltip.style.display = "none";
      return;
    }

    tooltip.innerHTML = html;
    tooltip.style.display = "block";

    const pad = 16;
    let left = e.clientX + pad;
    let top = e.clientY + pad;
    const rect = tooltip.getBoundingClientRect();
    if (left + rect.width > window.innerWidth) left = e.clientX - rect.width - pad;
    if (top + rect.height > window.innerHeight) top = e.clientY - rect.height - pad;
    tooltip.style.left = left + "px";
    tooltip.style.top = top + "px";
  });

  container.addEventListener("mouseleave", () => {
    tooltip.style.display = "none";
  });
}
