from __future__ import annotations

import json
import logging
import os
import uuid

import networkx as nx

logger = logging.getLogger(__name__)

EDGE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "dsec:graph:edge")


class GraphStore:
    """Loads serialized graph JSON into a NetworkX MultiDiGraph with
    lightweight secondary indices for frequent type-based lookups."""

    def __init__(self, json_path: str):
        self.G: nx.MultiDiGraph = nx.MultiDiGraph()
        self.tables_by_name: dict[str, str] = {}         # table_name -> urn
        self.nodes_by_type: dict[str, list[str]] = {}     # node_type -> [urn, ...]
        self.edges_by_type: dict[str, list[tuple[str, str, str]]] = {}  # relation_type -> [(from, to, key), ...]
        self.soft_links: list[dict] = []
        self.generated_at = "unknown"
        self._load(json_path)
        self._soft_links_path = os.path.join(
            os.path.dirname(json_path), "soft_links.json"
        )
        self._load_soft_links()

    def _load(self, json_path: str):
        with open(json_path) as f:
            data = json.load(f)

        self.generated_at = data.get("generated_at", "unknown")

        for node in data["nodes"]:
            urn = node["urn"]
            self.G.add_node(
                urn,
                node_type=node.get("node_type", "unknown"),
                parent_urn=node.get("parent_urn"),
                metadata=node.get("metadata", {}),
                organization_id=node.get("organization_id"),
            )
            node_type = node.get("node_type", "unknown")
            self.nodes_by_type.setdefault(node_type, []).append(urn)

            if node_type == "table":
                table_name = node.get("metadata", {}).get("table_name", "")
                if table_name:
                    self.tables_by_name[table_name] = urn

        for edge in data["edges"]:
            key = edge["uuid"]
            edge_type = edge.get("edge_type") or edge.get("relation_type", "UNKNOWN")
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

            edge_type = link.get("edge_type") or link.get("relation_type", "reads")
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
        tmp_path = self._soft_links_path + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump({"soft_links": self.soft_links}, f, indent=2)
            f.write("\n")
        os.replace(tmp_path, self._soft_links_path)

    def node_dict(self, urn: str) -> dict | None:
        """Return a node as a dict with urn/node_type/parent_urn/metadata
        keys, or None if the URN doesn't exist."""
        if urn not in self.G:
            return None
        attrs = self.G.nodes[urn]
        return {
            "urn": urn,
            "node_type": attrs.get("node_type", "unknown"),
            "parent_urn": attrs.get("parent_urn"),
            "metadata": attrs.get("metadata", {}),
        }
