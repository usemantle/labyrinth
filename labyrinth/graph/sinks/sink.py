from __future__ import annotations

import abc
from typing import Any

from labyrinth.graph.graph_models import Edge, Node


class Sink(abc.ABC):
    """Abstract interface for writing graph data to a persistent store."""

    @abc.abstractmethod
    def write(self, nodes: list[Node], edges: list[Edge]) -> None:
        """Persist nodes and edges."""
        ...

    @abc.abstractmethod
    def update_node_metadata(self, urn: str, **kwargs: Any) -> None:
        """Add or update metadata key-value pairs on a node."""
        ...

    @abc.abstractmethod
    def delete_node_metadata(self, urn: str, *keys: str) -> None:
        """Remove metadata keys from a node."""
        ...

    @abc.abstractmethod
    def add_soft_link(self, link: dict) -> None:
        """Append a soft link to the store."""
        ...

    @abc.abstractmethod
    def remove_soft_link(self, link_id: str) -> None:
        """Remove a soft link by its ID."""
        ...
