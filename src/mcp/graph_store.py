from __future__ import annotations

import json
import logging
import os
import threading
import uuid

import networkx as nx

from src.graph.graph_models import EdgeType, NodeType

logger = logging.getLogger(__name__)

EDGE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "dsec:graph:edge")


class GraphStore:
    """Loads serialized graph JSON into a NetworkX MultiDiGraph with
    lightweight secondary indices for frequent type-based lookups.

    A background watcher thread polls the graph file for changes and
    reloads automatically.  All reads must be done while holding
    ``self.lock`` (as a reader) and reloads acquire the write side so
    that stale reads are blocked during ingestion.
    """

    def __init__(self, json_path: str, *, poll_interval: float = 2.0):
        self.lock = threading.RLock()
        self._json_path = json_path
        self._poll_interval = poll_interval
        self._last_mtime: float = 0.0

        self.G: nx.MultiDiGraph = nx.MultiDiGraph()
        self.tables_by_name: dict[str, str] = {}         # table_name -> urn
        self.nodes_by_type: dict[str, list[str]] = {}     # node_type -> [urn, ...]
        self.edges_by_type: dict[str, list[tuple[str, str, str]]] = {}  # edge_type -> [(from, to, key), ...]
        self.soft_links: list[dict] = []
        self.generated_at = "unknown"
        self._load(json_path)
        self._soft_links_path = os.path.join(
            os.path.dirname(json_path), "soft_links.json"
        )
        self._load_soft_links()

        # Record mtime after initial load so the watcher doesn't
        # immediately trigger a redundant reload.
        try:
            self._last_mtime = os.path.getmtime(json_path)
        except OSError:
            pass

        # Start background watcher
        self._stop_event = threading.Event()
        self._watcher = threading.Thread(
            target=self._watch_loop, daemon=True, name="graph-watcher",
        )
        self._watcher.start()

    # ── File watching ────────────────────────────────────────────────

    def _watch_loop(self) -> None:
        """Poll the graph file for mtime changes and reload when detected."""
        while not self._stop_event.wait(self._poll_interval):
            try:
                mtime = os.path.getmtime(self._json_path)
            except OSError:
                continue
            if mtime != self._last_mtime:
                logger.info("Graph file changed — reloading")
                self.reload()

    def reload(self) -> None:
        """Re-read the graph JSON and soft links from disk.

        Acquires the write lock so that in-flight reads block until
        the new data is fully loaded.
        """
        with self.lock:
            # Reset indices
            self.G = nx.MultiDiGraph()
            self.tables_by_name = {}
            self.nodes_by_type = {}
            self.edges_by_type = {}
            self.soft_links = []
            self.generated_at = "unknown"

            self._load(self._json_path)
            self._load_soft_links()

            try:
                self._last_mtime = os.path.getmtime(self._json_path)
            except OSError:
                pass

    def stop_watcher(self) -> None:
        """Signal the watcher thread to exit."""
        self._stop_event.set()

    # ── Graph loading ─────────────────────────────────────────────────

    def _load(self, json_path: str):
        with open(json_path) as f:
            data = json.load(f)

        self.generated_at = data.get("generated_at", "unknown")

        for node in data["nodes"]:
            urn = node["urn"]
            self.G.add_node(
                urn,
                node_type=node.get("node_type", NodeType.UNKNOWN),
                parent_urn=node.get("parent_urn"),
                metadata=node.get("metadata", {}),
                organization_id=node.get("organization_id"),
            )
            node_type = node.get("node_type", NodeType.UNKNOWN)
            self.nodes_by_type.setdefault(node_type, []).append(urn)

            if node_type == NodeType.TABLE:
                table_name = node.get("metadata", {}).get("table_name", "")
                if table_name:
                    self.tables_by_name[table_name] = urn

        for edge in data["edges"]:
            key = edge["uuid"]
            edge_type = edge.get("edge_type", EdgeType.UNKNOWN)
            self.G.add_edge(
                edge["from_urn"],
                edge["to_urn"],
                key=key,
                edge_type=edge_type,
                metadata=edge.get("metadata", {}),
                organization_id=edge.get("organization_id"),
            )
            self.edges_by_type.setdefault(edge_type, []).append(
                (edge["from_urn"], edge["to_urn"], key)
            )

        logger.info(
            "Loaded graph: %d nodes, %d edges",
            self.G.number_of_nodes(),
            self.G.number_of_edges(),
        )

    def _load_soft_links(self):
        if not os.path.exists(self._soft_links_path):
            logger.info("No soft_links.json found — skipping")
            return

        with open(self._soft_links_path) as f:
            data = json.load(f)

        loaded = 0
        for link in data.get("soft_links", []):
            from_urn = link["from_urn"]
            to_urn = link["to_urn"]
            if from_urn not in self.G:
                logger.warning("Soft link skipped — from_urn not in graph: %s", from_urn)
                continue
            if to_urn not in self.G:
                logger.warning("Soft link skipped — to_urn not in graph: %s", to_urn)
                continue

            edge_type = link.get("edge_type", EdgeType.READS)
            edge_key = str(uuid.uuid5(
                EDGE_NAMESPACE, f"{from_urn}:{to_urn}:{edge_type}"
            ))
            org_id = self.G.nodes[from_urn].get("organization_id")

            self.G.add_edge(
                from_urn, to_urn, key=edge_key,
                edge_type=edge_type,
                metadata={
                    "detection_method": link.get("detection_method", "soft_link"),
                    "confidence": link.get("confidence", 0.7),
                    "note": link.get("note", ""),
                },
                organization_id=org_id,
            )
            self.edges_by_type.setdefault(edge_type, []).append(
                (from_urn, to_urn, edge_key)
            )
            self.soft_links.append(link)
            loaded += 1

        logger.info("Loaded %d soft link(s)", loaded)

    def _save_soft_links(self):
        with self.lock:
            tmp_path = self._soft_links_path + ".tmp"
            with open(tmp_path, "w") as f:
                json.dump({"soft_links": self.soft_links}, f, indent=2)
                f.write("\n")
            os.replace(tmp_path, self._soft_links_path)

    def node_dict(self, urn: str) -> dict | None:
        """Return a node as a dict with urn/node_type/parent_urn/metadata
        keys, or None if the URN doesn't exist."""
        with self.lock:
            if urn not in self.G:
                return None
            attrs = self.G.nodes[urn]
            return {
                "urn": urn,
                "node_type": attrs.get("node_type", NodeType.UNKNOWN),
                "parent_urn": attrs.get("parent_urn"),
                "metadata": attrs.get("metadata", {}),
            }
