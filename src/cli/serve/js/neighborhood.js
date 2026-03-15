/**
 * Click-to-focus node, degree slider (1-5), clear button, BFS computation.
 */
import state from "./state.js";
import { NODE_STYLES } from "./styles.js";

/** BFS to find all nodes within N hops, skipping `contains` edges.
 *  `contains` edges are organizational hierarchy and don't represent
 *  semantic relationships — traversing them pulls in entire subtrees
 *  of unrelated siblings from root container nodes. */
function bfs(startNode, maxDegree) {
  const visited = new Set([startNode]);
  let frontier = [startNode];

  for (let d = 0; d < maxDegree; d++) {
    const nextFrontier = [];
    for (const node of frontier) {
      state.graph.forEachEdge(node, (edge, attrs, src, tgt) => {
        if (attrs._relationType === "contains") return;
        const neighbor = src === node ? tgt : src;
        if (!visited.has(neighbor)) {
          visited.add(neighbor);
          nextFrontier.push(neighbor);
        }
      });
    }
    frontier = nextFrontier;
    if (frontier.length === 0) break;
  }

  return visited;
}

export function recomputeFocus() {
  if (!state.focusedNode || !state.graph || !state.graph.hasNode(state.focusedNode)) {
    state.focusedSubgraph = new Set();
    return;
  }
  state.focusedSubgraph = bfs(state.focusedNode, state.focusDegree);
}

function updateFocusDisplay() {
  const label = document.getElementById("focus-label");
  const controls = document.getElementById("focus-controls");
  const degreeValue = document.getElementById("degree-value");

  if (state.focusedNode) {
    const nodeLabel = state.graph
      ? state.graph.getNodeAttribute(state.focusedNode, "label") || state.focusedNode
      : state.focusedNode;
    const nodeType = state.graph
      ? state.graph.getNodeAttribute(state.focusedNode, "_type") || "unknown"
      : "unknown";
    const style = NODE_STYLES[nodeType] || NODE_STYLES.unknown;

    label.innerHTML = `<span class="tt-badge" style="background:${style.color};font-size:10px">${nodeType}</span> ${nodeLabel}`;
    controls.style.display = "block";
    degreeValue.textContent = state.focusDegree;
  } else {
    label.textContent = "Click a node to focus";
    controls.style.display = "none";
  }
}

export function setupNeighborhood() {
  const slider = document.getElementById("degree-slider");
  const clearBtn = document.getElementById("clear-focus");

  slider.addEventListener("input", () => {
    state.focusDegree = parseInt(slider.value, 10);
    recomputeFocus();
    updateFocusDisplay();
    state.requestRefresh();
  });

  clearBtn.addEventListener("click", () => {
    state.focusedNode = null;
    state.focusedSubgraph = new Set();
    slider.value = 1;
    state.focusDegree = 1;
    updateFocusDisplay();
    state.requestRefresh();
  });
}

export function onNodeClick(nodeKey) {
  state.focusedNode = nodeKey;
  state.focusDegree = parseInt(document.getElementById("degree-slider").value, 10);
  recomputeFocus();
  updateFocusDisplay();
  state.requestRefresh();
}
