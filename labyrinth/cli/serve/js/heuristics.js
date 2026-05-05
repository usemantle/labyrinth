/**
 * Heuristics tab: fetch heuristics.json and render candidate table.
 */

export async function initHeuristics() {
  const container = document.getElementById("heuristics-content");
  const emptyEl = document.getElementById("heuristics-empty");

  try {
    const resp = await fetch("heuristics.json");
    if (!resp.ok) {
      console.warn("[labyrinth] heuristics.json fetch failed:", resp.status);
      return;
    }
    const data = await resp.json();
    const candidates = data.candidates || [];

    if (candidates.length === 0) {
      return; // keep the empty message
    }

    emptyEl.style.display = "none";

    // Header
    const header = document.createElement("div");
    header.className = "heuristics-header";
    header.innerHTML = `
      <h2 style="font-size:16px;font-weight:700;color:#cdd6f4;margin-bottom:4px">Heuristic Analysis</h2>
      <div class="timestamp">Analyzed: ${data.analyzed_at || "unknown"}</div>
      <div class="timestamp">Graph generated: ${data.graph_generated_at || "unknown"}</div>
    `;
    container.appendChild(header);

    // Summary cards
    const byHeuristic = {};
    for (const c of candidates) {
      byHeuristic[c.heuristic_name] = (byHeuristic[c.heuristic_name] || 0) + 1;
    }
    const cards = document.createElement("div");
    cards.className = "summary-cards";
    cards.innerHTML = `
      <div class="summary-card">
        <div class="card-value" style="color:#89b4fa">${candidates.length}</div>
        <div class="card-label">Total Candidates</div>
      </div>
      ${Object.entries(byHeuristic)
        .sort()
        .map(
          ([name, count]) => `
        <div class="summary-card">
          <div class="card-value" style="color:#cba6f7">${count}</div>
          <div class="card-label">${name}</div>
        </div>
      `
        )
        .join("")}
    `;
    container.appendChild(cards);

    // Candidate table
    const table = document.createElement("table");
    table.className = "heuristics-table";
    table.innerHTML = `
      <thead>
        <tr>
          <th>ID</th>
          <th>Heuristic</th>
          <th>Source URN</th>
          <th>Node Type</th>
          <th>Actions</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody id="heuristics-tbody"></tbody>
    `;
    container.appendChild(table);

    const tbody = document.getElementById("heuristics-tbody");
    for (const c of candidates) {
      const shortId = c.id.substring(0, 12);
      const shortUrn =
        c.source_urn.length > 60
          ? "..." + c.source_urn.substring(c.source_urn.length - 57)
          : c.source_urn;

      const row = document.createElement("tr");
      row.innerHTML = `
        <td class="uuid-cell" title="${c.id}">${shortId}</td>
        <td><span class="heuristic-badge">${c.heuristic_name}</span></td>
        <td title="${c.source_urn}" style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${shortUrn}</td>
        <td>${c.source_node_type}</td>
        <td>${(c.terminal_actions || []).map((a) => `<span class="action-badge MCP_TOOL_CALL">${a}</span>`).join(" ")}</td>
        <td><span class="status-badge ${c.status || "pending"}">${c.status || "pending"}</span></td>
      `;
      tbody.appendChild(row);

      // Click UUID to copy
      const uuidCell = row.querySelector(".uuid-cell");
      uuidCell.addEventListener("click", () => {
        navigator.clipboard.writeText(c.id);
        uuidCell.textContent = "copied!";
        setTimeout(() => {
          uuidCell.textContent = shortId;
        }, 1200);
      });

      // Expandable metadata row
      const metaRow = document.createElement("tr");
      metaRow.className = "metadata-row";
      metaRow.innerHTML = `<td colspan="6" class="metadata-cell">${JSON.stringify(c.source_metadata, null, 2)}</td>`;
      tbody.appendChild(metaRow);

      row.style.cursor = "pointer";
      row.addEventListener("click", (e) => {
        if (e.target.classList.contains("uuid-cell")) return;
        metaRow.classList.toggle("open");
      });
    }
  } catch (err) {
    console.error("[labyrinth] initHeuristics error:", err);
  }
}
