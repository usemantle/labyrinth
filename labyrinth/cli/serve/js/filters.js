/**
 * Sidebar: node/edge type toggles grouped by domain, layout picker.
 */
import state from "./state.js";
import { NODE_STYLES, EDGE_STYLES, DOMAIN_GROUPS } from "./styles.js";

export function buildFilters(nodeTypeIndex) {
  buildNodeFilters(nodeTypeIndex);
  buildEdgeFilters();

  document.getElementById("hide-structural").addEventListener("change", (e) => {
    state.hideStructural = e.target.checked;
    state.requestRebuild();
  });

  document.getElementById("layout-select").addEventListener("change", (e) => {
    state.currentLayout = e.target.value;
    state.requestRebuild();
  });
}

function buildNodeFilters(nodeTypeIndex) {
  const container = document.getElementById("node-filters");
  container.innerHTML = "";

  // Group types by domain
  const assigned = new Set();
  for (const [groupName, types] of Object.entries(DOMAIN_GROUPS)) {
    const presentTypes = types.filter((t) => nodeTypeIndex[t]);
    if (presentTypes.length === 0) continue;

    const details = document.createElement("details");
    details.open = true;

    const summary = document.createElement("summary");
    summary.textContent = groupName;
    details.appendChild(summary);

    for (const type of presentTypes) {
      assigned.add(type);
      details.appendChild(createNodeFilterRow(type, nodeTypeIndex[type].length));
    }

    container.appendChild(details);
  }

  // Ungrouped types
  const ungrouped = Object.keys(nodeTypeIndex)
    .filter((t) => !assigned.has(t))
    .sort();
  if (ungrouped.length > 0) {
    const details = document.createElement("details");
    details.open = true;
    const summary = document.createElement("summary");
    summary.textContent = "Other";
    details.appendChild(summary);
    for (const type of ungrouped) {
      details.appendChild(createNodeFilterRow(type, nodeTypeIndex[type].length));
    }
    container.appendChild(details);
  }
}

function createNodeFilterRow(type, count) {
  const style = NODE_STYLES[type] || NODE_STYLES.unknown;
  const row = document.createElement("div");
  row.className = "filter-row";
  row.innerHTML = `
    <input type="checkbox" id="nt-${type}" checked data-node-type="${type}" />
    <span class="swatch" style="background:${style.color}"></span>
    <label for="nt-${type}">${type}</label>
    <span class="count">${count}</span>
  `;
  row.querySelector("input").addEventListener("change", (e) => {
    if (e.target.checked) {
      state.hiddenNodeTypes.delete(type);
    } else {
      state.hiddenNodeTypes.add(type);
    }
    state.requestRebuild();
  });
  return row;
}

function buildEdgeFilters() {
  const container = document.getElementById("edge-filters");
  container.innerHTML = "";

  // Only show edge types that are actually present
  const presentEdgeTypes = new Set(state.allEdges.map((e) => e.edge_type));

  for (const type of Object.keys(EDGE_STYLES).sort()) {
    if (!presentEdgeTypes.has(type)) continue;
    const style = EDGE_STYLES[type];
    const row = document.createElement("div");
    row.className = "filter-row";
    row.innerHTML = `
      <input type="checkbox" id="et-${type}" checked data-edge-type="${type}" />
      <span class="swatch" style="background:${style.color}"></span>
      <label for="et-${type}">${type}</label>
    `;
    row.querySelector("input").addEventListener("change", (e) => {
      if (e.target.checked) {
        state.hiddenEdgeTypes.delete(type);
      } else {
        state.hiddenEdgeTypes.add(type);
      }
      state.requestRebuild();
    });
    container.appendChild(row);
  }
}
