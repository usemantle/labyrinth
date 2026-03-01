from __future__ import annotations

import abc

from src.graph.graph_models import Edge, Node


class Sink(abc.ABC):
    """Abstract interface for writing graph data to a persistent store."""

    @abc.abstractmethod
    def write(self, nodes: list[Node], edges: list[Edge]) -> None:
        """Persist nodes and edges."""
        ...
