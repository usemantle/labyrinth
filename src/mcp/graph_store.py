from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from pathlib import Path

import networkx as nx

from src.graph.graph_models import EdgeType, NodeType
from src.graph.sinks.json_file_sink import JsonFileSink
from src.graph.store import EDGE_NAMESPACE, GraphStoreBase

logger = logging.getLogger(__name__)


class GraphStore(GraphStoreBase):
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
        self.tables_by_name: dict[str, str] = {}
        self.nodes_by_type: dict[str, list[str]] = {}
        self.edges_by_type: dict[str, list[tuple[str, str, str]]] = {}
        self._soft_links: list[dict] = []
        self.generated_at = "unknown"

        self.sink = JsonFileSink(Path(json_path))

        self._load()

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
        """Re-read the graph JSON from disk."""
        super().reload()
        try:
            self._last_mtime = os.path.getmtime(self._json_path)
        except OSError:
            pass

    def stop_watcher(self) -> None:
        """Signal the watcher thread to exit."""
        self._stop_event.set()

    # ── Graph loading ─────────────────────────────────────────────────

    def _load(self) -> None:
        with open(self._json_path) as f:
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
            type_list = self.nodes_by_type.setdefault(node_type, [])
            if urn not in type_list:
                type_list.append(urn)

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

        # Load soft links from the same file
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
            self._soft_links.append(link)

        logger.info(
            "Loaded graph: %d nodes, %d edges, %d soft links",
            self.G.number_of_nodes(),
            self.G.number_of_edges(),
            len(self._soft_links),
        )
