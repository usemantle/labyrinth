"""Abstract base class defining the graph store contract."""

from __future__ import annotations

import logging
import threading
import uuid
from abc import ABC, abstractmethod
from typing import Any

import networkx as nx

from labyrinth.graph.graph_models import EdgeType, NodeType
from labyrinth.graph.sinks.sink import Sink

logger = logging.getLogger(__name__)

EDGE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "dsec:graph:edge")


class GraphStoreBase(ABC):
    """Base class for graph store implementations.

    Provides concrete implementations for mutations, queries, and reload
    logic.  Subclasses must implement :meth:`_load` to populate the
    in-memory graph from their backing store.

    Subclass ``__init__`` must initialise at least:

    - ``self.sink`` – a :class:`~labyrinth.graph.sinks.sink.Sink` for persistence.
    - ``self.G`` – a :class:`~networkx.MultiDiGraph`.
    - ``self.lock`` – a :class:`threading.RLock`.
    - ``self.nodes_by_type``, ``self.edges_by_type``, ``self.tables_by_name``
    - ``self._soft_links``
    - ``self.generated_at``
    """

    sink: Sink
    G: nx.MultiDiGraph
    lock: threading.RLock
    nodes_by_type: dict[str, list[str]]
    edges_by_type: dict[str, list[tuple[str, str, str]]]
    tables_by_name: dict[str, str]
    generated_at: str

    # ── Abstract ──────────────────────────────────────────────────────

    @abstractmethod
    def _load(self) -> None:
        """Populate ``self.G`` and secondary indices from the backing store."""
        ...

    # ── Properties ────────────────────────────────────────────────────

    @property
    def soft_links(self) -> list[dict]:
        return self._soft_links

    # ── Reload ────────────────────────────────────────────────────────

    def reload(self) -> None:
        """Reset indices and re-populate the graph from the backing store.

        Acquires the write lock so that in-flight reads block until the
        new data is fully loaded.
        """
        with self.lock:
            self.G = nx.MultiDiGraph()
            self.tables_by_name = {}
            self.nodes_by_type = {}
            self.edges_by_type = {}
            self._soft_links: list[dict] = []
            self.generated_at = "unknown"
            self._load()

    def stop_watcher(self) -> None:  # noqa: B027
        """Stop a background file-watcher, if any. No-op by default."""

    # ── Mutation methods ──────────────────────────────────────────────

    def update_node_metadata(self, urn: str, **kwargs: Any) -> None:
        """Update metadata on a node in-memory and persist."""
        with self.lock:
            if urn not in self.G:
                raise KeyError(f"Node not found: {urn}")
            meta = self.G.nodes[urn].get("metadata", {})
            meta.update(kwargs)
            self.G.nodes[urn]["metadata"] = meta
            self.sink.update_node_metadata(urn, **kwargs)

    def delete_node_metadata(self, urn: str, *keys: str) -> None:
        """Remove metadata keys from a node in-memory and persist."""
        with self.lock:
            if urn not in self.G:
                raise KeyError(f"Node not found: {urn}")
            meta = self.G.nodes[urn].get("metadata", {})
            for k in keys:
                meta.pop(k, None)
            self.sink.delete_node_metadata(urn, *keys)

    def add_soft_link(self, link: dict) -> None:
        """Add a soft link to the in-memory graph and persist."""
        with self.lock:
            from_urn = link["from_urn"]
            to_urn = link["to_urn"]
            edge_type = link.get("edge_type", EdgeType.READS)
            edge_key = str(uuid.uuid5(
                EDGE_NAMESPACE, f"{from_urn}:{to_urn}:{edge_type}",
            ))
            org_id = self.G.nodes[from_urn].get("organization_id")

            self.G.add_edge(
                from_urn, to_urn, key=edge_key,
                edge_type=edge_type,
                metadata={
                    "detection_method": "soft_link",
                    "confidence": link.get("confidence", 0.7),
                    "note": link.get("note", ""),
                },
                organization_id=org_id,
            )
            self.edges_by_type.setdefault(edge_type, []).append(
                (from_urn, to_urn, edge_key),
            )
            self._soft_links.append(link)
            self.sink.add_soft_link(link)

    def remove_soft_link(self, link_id: str) -> None:
        """Remove a soft link from the in-memory graph and persist."""
        with self.lock:
            target_link = None
            for link in self._soft_links:
                if link["id"] == link_id:
                    target_link = link
                    break

            if target_link is None:
                raise KeyError(f"No soft link found with id={link_id}")

            from_urn = target_link["from_urn"]
            to_urn = target_link["to_urn"]
            edge_type = target_link.get("edge_type", EdgeType.SOFT_REFERENCE)
            edge_key = str(uuid.uuid5(
                EDGE_NAMESPACE, f"{from_urn}:{to_urn}:{edge_type}",
            ))

            if self.G.has_edge(from_urn, to_urn, key=edge_key):
                self.G.remove_edge(from_urn, to_urn, key=edge_key)

            edge_tuple = (from_urn, to_urn, edge_key)
            if edge_type in self.edges_by_type:
                try:
                    self.edges_by_type[edge_type].remove(edge_tuple)
                except ValueError:
                    pass

            self._soft_links.remove(target_link)
            self.sink.remove_soft_link(link_id)

    # ── Query helpers ─────────────────────────────────────────────────

    def node_dict(self, urn: str) -> dict | None:
        """Return a node as a dict, or None if the URN doesn't exist."""
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
