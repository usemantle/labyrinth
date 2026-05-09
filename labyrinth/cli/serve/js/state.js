/**
 * Shared state object — single source of truth for all modules.
 * Modules mutate state directly and call requestRefresh() or requestRebuild().
 */
const state = {
  // Data
  allNodes: [],
  allEdges: [],
  softLinkIds: new Set(),

  // Graphology / Sigma instances (set by graph-app.js)
  graph: null,
  sigma: null,

  // Filters
  hiddenNodeTypes: new Set(),
  hiddenEdgeTypes: new Set(),
  hideStructural: true,

  // Hover
  hoveredNode: null,
  hoveredEdge: null,
  hoveredNeighbors: new Set(),

  // Focus (neighborhood)
  focusedNode: null,
  focusDegree: 1,
  focusedSubgraph: new Set(),

  // Search
  searchQuery: "",
  activeMetadataFilter: "",

  // Layout
  currentLayout: "forceatlas2",

  // Mouse position (for edge tooltips)
  mouseX: 0,
  mouseY: 0,

  // Callbacks (set by graph-app.js)
  requestRefresh: () => {},
  requestRebuild: () => {},
};

export default state;
