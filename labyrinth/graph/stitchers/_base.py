"""Abstract base classes for graph stitchers and resolvers."""

from __future__ import annotations

import abc
import uuid
from dataclasses import dataclass, field

from labyrinth.graph.graph_models import URN, Graph, Node


@dataclass
class NodeIndex:
    """Pre-filtered node collections built by Stitcher.index_nodes().

    Provides nodes grouped by type and lookup dicts keyed by metadata values.
    """

    by_type: dict[str, list[Node]] = field(default_factory=dict)
    by_metadata: dict[str, dict[str, URN]] = field(default_factory=dict)

    def nodes_of_type(self, node_type: str) -> list[Node]:
        """Return all nodes matching the given type."""
        return self.by_type.get(node_type, [])

    def lookup(self, key: str) -> dict[str, URN]:
        """Return the metadata-value -> URN mapping for the given metadata key."""
        return self.by_metadata.get(key, {})


class Stitcher(abc.ABC):
    """A stitcher reads the graph and returns new nodes/edges without mutating the input."""

    def index_nodes(self, graph: Graph, *, types: set[str] | None = None, metadata_keys: set[str] | None = None) -> NodeIndex:
        """Build a NodeIndex from the graph by filtering on types and indexing by metadata keys.

        Args:
            graph: The full graph to index.
            types: If provided, only nodes with node_type in this set are collected into by_type.
                   If None, all nodes are grouped by type.
            metadata_keys: If provided, for each key build a dict mapping the metadata value
                           to the node's URN. Only the first occurrence of each value is kept.

        Returns:
            A NodeIndex with the requested groupings.
        """
        index = NodeIndex()
        for node in graph.nodes:
            if types is None or node.node_type in types:
                index.by_type.setdefault(node.node_type, []).append(node)

            if metadata_keys:
                for mk in metadata_keys:
                    if mk in node.metadata:
                        bucket = index.by_metadata.setdefault(mk, {})
                        val = node.metadata[mk]
                        if isinstance(val, str) and val not in bucket:
                            bucket[val] = node.urn

        return index

    @abc.abstractmethod
    def stitch(self, organization_id: uuid.UUID, graph: Graph, context: dict) -> Graph:
        """Return a Graph containing ONLY new nodes/edges."""
        ...


class Resolver(abc.ABC):
    """A resolver mutates existing edges in the graph (e.g., fixing placeholder URNs)."""

    @abc.abstractmethod
    def resolve(self, organization_id: uuid.UUID, graph: Graph, context: dict) -> Graph:
        """Mutate and return the full graph."""
        ...
