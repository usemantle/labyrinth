/**
 * Main orchestrator: fetch data, build graph, create Sigma, wire modules.
 */
import Graph from "https://esm.sh/graphology@0.25.4";
import Sigma from "https://esm.sh/sigma@3.0.2";
import circular from "https://esm.sh/graphology-layout@0.6.1/circular";
import random from "https://esm.sh/graphology-layout@0.6.1/random";
import forceAtlas2 from "https://esm.sh/graphology-layout-forceatlas2@0.10.1";

import state from "./state.js";
import { NODE_STYLES, EDGE_STYLES, STRUCTURAL_SOURCES } from "./styles.js";
import { urnLabel } from "./utils.js";
import { buildFilters } from "./filters.js";
import { setupSearch, nodeMatchesSearch } from "./search.js";
import { setupNeighborhood, onNodeClick, recomputeFocus } from "./neighborhood.js";
import { setupTooltip } from "./tooltip.js";

let EdgeCurveProgram = null;

// Try to load edge-curve program for soft link rendering
async function loadEdgeCurve() {
  try {
    const mod = await import("https://esm.sh/@sigma/edge-curve@3.0.2");
    EdgeCurveProgram = mod.default || mod.EdgeCurveProgram;
  } catch (e) {
    console.warn("[labyrinth] @sigma/edge-curve not available, using straight arrows for soft links");
  }
}

// ── Layout ──
function applyLayout(graph, layout) {
  switch (layout) {
    case "circular":
      circular.assign(graph);
      break;
    case "random":
      random.assign(graph);
      break;
    case "forceatlas2":
    default:
      circular.assign(graph);
      forceAtlas2.assign(graph, {
        iterations: 80,
        settings: {
          barnesHutOptimize: true,
          strongGravityMode: false,
          gravity: 1,
          scalingRatio: 80,
        },
      });
      break;
  }
}

// ── Reducers ──
function nodeReducer(node, data) {
  const res = { ...data };

  // 1. Focus mode: hide nodes outside subgraph
  if (state.focusedNode) {
    if (!state.focusedSubgraph.has(node)) {
      res.hidden = true;
      return res;
    }
  }

  // 2. Search/tag filter: dim non-matches
  if (state.searchQuery || state.activeMetadataFilter) {
    if (!nodeMatchesSearch(node)) {
      res.color = "#313244";
      res.label = "";
      res.zIndex = 0;
      return res;
    }
  }

  // 3. Hover: dim non-neighbors
  if (state.hoveredNode) {
    if (node === state.hoveredNode) {
      res.highlighted = true;
      res.zIndex = 10;
    } else if (state.hoveredNeighbors.has(node)) {
      res.zIndex = 1;
    } else {
      res.color = "#313244";
      res.label = "";
      res.zIndex = 0;
    }
  }

  return res;
}

function edgeReducer(edge, data) {
  const res = { ...data };

  // 1. Focus mode: hide edges outside subgraph
  if (state.focusedNode && state.graph) {
    const src = state.graph.source(edge);
    const tgt = state.graph.target(edge);
    if (!state.focusedSubgraph.has(src) || !state.focusedSubgraph.has(tgt)) {
      res.hidden = true;
      return res;
    }
  }

  // 2. Hovered edge: highlight
  if (state.hoveredEdge === edge) {
    res.size = (res.size || 1) * 2.5;
    res.zIndex = 10;
    return res;
  }

  // 3. Hovered node: hide edges not touching it
  if (state.hoveredNode && state.graph) {
    const src = state.graph.source(edge);
    const tgt = state.graph.target(edge);
    if (src === state.hoveredNode || tgt === state.hoveredNode) {
      res.size = (res.size || 1) * 2;
      res.zIndex = 10;
    } else {
      res.hidden = true;
    }
  }

  return res;
}

// ── Build graph ──
function buildGraph() {
  console.log("[labyrinth] buildGraph() starting");
  if (state.sigma) {
    state.sigma.kill();
    state.sigma = null;
  }

  const graph = new Graph({ multi: true, type: "directed" });
  state.graph = graph;

  const nodeTypeIndex = {};

  // Hidden node URNs (by type)
  const hiddenUrns = new Set();
  for (const n of state.allNodes) {
    if (state.hiddenNodeTypes.has(n.node_type)) {
      hiddenUrns.add(n.urn);
    }
  }

  // Add nodes
  for (const n of state.allNodes) {
    if (hiddenUrns.has(n.urn)) continue;
    if (graph.hasNode(n.urn)) continue;
    const style = NODE_STYLES[n.node_type] || NODE_STYLES.unknown;
    nodeTypeIndex[n.node_type] = nodeTypeIndex[n.node_type] || [];
    nodeTypeIndex[n.node_type].push(n.urn);

    graph.addNode(n.urn, {
      label: urnLabel(n.urn),
      size: style.size,
      color: style.color,
      x: Math.random() * 100,
      y: Math.random() * 100,
      _type: n.node_type,
      _metadata: n.metadata,
    });
  }

  // Add edges
  let edgeIdx = 0;
  for (const e of state.allEdges) {
    if (!graph.hasNode(e.from_urn) || !graph.hasNode(e.to_urn)) continue;
    if (state.hiddenEdgeTypes.has(e.edge_type)) continue;

    const isStructural =
      e.edge_type === "contains" &&
      STRUCTURAL_SOURCES.has(graph.getNodeAttribute(e.from_urn, "_type"));
    if (state.hideStructural && isStructural) continue;

    const style = EDGE_STYLES[e.edge_type] || EDGE_STYLES.contains;
    const isSoftLink = state.softLinkIds.has(e.uuid) || (e.metadata && e.metadata.detection_method === "soft_link");
    let edgeType = "arrow";
    if (isSoftLink && EdgeCurveProgram) {
      edgeType = "curvedArrow";
    }

    graph.addEdgeWithKey(`e-${edgeIdx++}`, e.from_urn, e.to_urn, {
      color: isSoftLink ? "#f9a8d4" : style.color,
      size: isSoftLink ? Math.max(style.size, 2) : style.size,
      type: edgeType,
      forceLabel: false,
      _relationType: e.edge_type,
      _metadata: e.metadata,
      _uuid: e.uuid,
    });
  }

  // Layout
  applyLayout(graph, state.currentLayout);

  // Sigma options
  const sigmaOpts = {
    renderLabels: true,
    labelColor: { color: "#cdd6f4" },
    labelSize: 10,
    labelRenderedSizeThreshold: 6,
    defaultEdgeType: "arrow",
    enableEdgeClickEvents: true,
    enableEdgeHoverEvents: true,
    nodeReducer,
    edgeReducer,
  };

  if (EdgeCurveProgram) {
    sigmaOpts.edgeProgramClasses = { curvedArrow: EdgeCurveProgram };
  }

  const container = document.getElementById("sigma-container");
  state.sigma = new Sigma(graph, container, sigmaOpts);

  // ── Events ──
  state.sigma.on("enterNode", ({ node }) => {
    state.hoveredNode = node;
    state.hoveredNeighbors = new Set(graph.neighbors(node));
    state.sigma.refresh();
  });

  state.sigma.on("leaveNode", () => {
    state.hoveredNode = null;
    state.hoveredNeighbors = new Set();
    state.sigma.refresh();
  });

  state.sigma.on("enterEdge", ({ edge }) => {
    state.hoveredEdge = edge;
    state.sigma.refresh();
  });

  state.sigma.on("leaveEdge", () => {
    state.hoveredEdge = null;
    state.sigma.refresh();
  });

  state.sigma.on("clickNode", ({ node }) => {
    onNodeClick(node);
  });

  console.log(`[labyrinth] buildGraph() done — ${graph.order} nodes, ${graph.size} edges`);
  return nodeTypeIndex;
}

// ── Stats ──
function updateStats() {
  const edgeCounts = {};
  for (const e of state.allEdges) {
    edgeCounts[e.edge_type] = (edgeCounts[e.edge_type] || 0) + 1;
  }
  const statsEl = document.getElementById("stats");
  let html = `<span>Nodes: ${state.allNodes.length}</span><span>Edges: ${state.allEdges.length}</span>`;
  for (const [k, v] of Object.entries(edgeCounts).sort()) {
    const style = EDGE_STYLES[k] || EDGE_STYLES.contains;
    html += `<span><span class="stat-dot" style="background:${style.color}"></span>${k}: ${v}</span>`;
  }
  statsEl.innerHTML = html;
}

// ── Main ──
async function main() {
  console.log("[labyrinth] main() starting");
  try {
    // Load edge curve program
    await loadEdgeCurve();

    // Fetch graph data
    const resp = await fetch("graph_data.json");
    if (!resp.ok) {
      console.error("[labyrinth] graph_data.json fetch failed:", resp.status);
      return;
    }
    const data = await resp.json();
    state.allNodes = data.nodes;
    state.allEdges = data.edges;
    console.log(`[labyrinth] Loaded ${state.allNodes.length} nodes, ${state.allEdges.length} edges`);

    // Merge soft links from inline data
    const nodeUrns = new Set(state.allNodes.map((n) => n.urn));
    for (const link of data.soft_links || []) {
      if (!nodeUrns.has(link.from_urn) || !nodeUrns.has(link.to_urn)) continue;
      state.softLinkIds.add(link.id);
      state.allEdges.push({
        uuid: link.id,
        from_urn: link.from_urn,
        to_urn: link.to_urn,
        edge_type: link.edge_type || "reads",
        metadata: {
          detection_method: link.detection_method || "soft_link",
          confidence: link.confidence,
          note: link.note,
        },
      });
    }

    // Set up rebuild/refresh callbacks
    state.requestRefresh = () => {
      if (state.sigma) state.sigma.refresh();
    };
    state.requestRebuild = () => {
      const idx = buildGraph();
      recomputeFocus();
      buildFilters(idx);
      updateStats();
    };

    // Initial build
    const nodeTypeIndex = buildGraph();
    buildFilters(nodeTypeIndex);
    updateStats();
    setupSearch();
    setupNeighborhood();
    setupTooltip();

    // Sidebar toggle
    document.getElementById("toggle-btn").addEventListener("click", () => {
      document.getElementById("sidebar").classList.toggle("collapsed");
      document.getElementById("toggle-btn").classList.toggle("shifted");
    });

    // Tab switching
    setupTabs();
  } catch (err) {
    console.error("[labyrinth] main() error:", err);
    document.getElementById("stats").textContent = "Error: " + err.message;
  }
}

function setupTabs() {
  const tabBtns = document.querySelectorAll(".tab-btn");
  const tabContents = {
    graph: document.getElementById("tab-graph"),
    actions: document.getElementById("tab-actions"),
  };

  tabBtns.forEach((btn) => {
    btn.addEventListener("click", async () => {
      const tab = btn.dataset.tab;

      // Update active tab button
      tabBtns.forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");

      // Toggle tab content
      Object.values(tabContents).forEach((el) => el.classList.remove("active"));
      if (tabContents[tab]) tabContents[tab].classList.add("active");

      // Lazy-load dashboard on first actions tab click
      if (tab === "actions" && !state._dashboardLoaded) {
        state._dashboardLoaded = true;
        const { initDashboard } = await import("./dashboard.js");
        initDashboard();
      }

      // Refresh Sigma when returning to graph tab
      if (tab === "graph" && state.sigma) {
        state.sigma.refresh();
      }
    });
  });
}

main();
