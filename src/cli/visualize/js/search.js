/**
 * Search box (URN substring) + metadata tag filter dropdown.
 */
import state from "./state.js";

/** Collect all unique metadata keys across all nodes. */
function collectMetadataKeys() {
  const keys = new Set();
  for (const n of state.allNodes) {
    if (n.metadata) {
      for (const k of Object.keys(n.metadata)) keys.add(k);
    }
  }
  return [...keys].sort();
}

export function setupSearch() {
  const searchInput = document.getElementById("search-input");
  const tagSelect = document.getElementById("tag-filter");
  const resultsDiv = document.getElementById("search-results");

  // Populate tag filter
  const keys = collectMetadataKeys();
  tagSelect.innerHTML = '<option value="">All nodes</option>';
  for (const k of keys) {
    const opt = document.createElement("option");
    opt.value = k;
    opt.textContent = `Has ${k}`;
    tagSelect.appendChild(opt);
  }

  searchInput.addEventListener("input", () => {
    state.searchQuery = searchInput.value.toLowerCase().trim();
    updateSearchResults();
    state.requestRefresh();
  });

  tagSelect.addEventListener("change", () => {
    state.activeMetadataFilter = tagSelect.value;
    state.requestRefresh();
  });
}

function updateSearchResults() {
  const resultsDiv = document.getElementById("search-results");
  resultsDiv.innerHTML = "";

  if (!state.searchQuery || !state.graph) {
    resultsDiv.style.display = "none";
    return;
  }

  const matches = [];
  state.graph.forEachNode((node, attrs) => {
    if (matches.length >= 10) return;
    if (node.toLowerCase().includes(state.searchQuery)) {
      matches.push({ key: node, label: attrs.label });
    }
  });

  if (matches.length === 0) {
    resultsDiv.style.display = "none";
    return;
  }

  resultsDiv.style.display = "block";
  for (const m of matches) {
    const item = document.createElement("div");
    item.className = "search-result-item";
    item.textContent = m.label;
    item.title = m.key;
    item.addEventListener("click", () => {
      const pos = state.sigma.getNodeDisplayData(m.key);
      if (pos) {
        state.sigma.getCamera().animate(pos, { duration: 300 });
      }
      resultsDiv.style.display = "none";
      searchInput.value = "";
      state.searchQuery = "";
      state.requestRefresh();
    });
    resultsDiv.appendChild(item);
  }
}

/** Check if a node matches the current search + tag filter criteria. */
export function nodeMatchesSearch(nodeKey) {
  if (!state.searchQuery && !state.activeMetadataFilter) return true;

  let matchesQuery = true;
  if (state.searchQuery) {
    matchesQuery = nodeKey.toLowerCase().includes(state.searchQuery);
  }

  let matchesTag = true;
  if (state.activeMetadataFilter && state.graph) {
    const meta = state.graph.getNodeAttribute(nodeKey, "_metadata") || {};
    matchesTag = state.activeMetadataFilter in meta;
  }

  return matchesQuery && matchesTag;
}
