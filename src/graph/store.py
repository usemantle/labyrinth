"""Abstract base class defining the graph store contract."""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from typing import Any

import networkx as nx


class GraphStoreBase(ABC):
    """Contract that any graph store implementation must satisfy.

    Implementations must initialise the following instance attributes:

    - ``G`` – a :class:`~networkx.MultiDiGraph` holding the in-memory graph.
    - ``lock`` – a :class:`threading.RLock` guarding concurrent access.
    - ``nodes_by_type`` – secondary index mapping *node_type* → list of URNs.
    - ``edges_by_type`` – secondary index mapping *edge_type* → list of
      ``(from_urn, to_urn, key)`` tuples.
    - ``tables_by_name`` – mapping *table_name* → URN for table nodes.
    - ``generated_at`` – timestamp string from the serialised graph.
    """

    G: nx.MultiDiGraph
    lock: threading.RLock
    nodes_by_type: dict[str, list[str]]
    edges_by_type: dict[str, list[tuple[str, str, str]]]
    tables_by_name: dict[str, str]
    generated_at: str

    @property
    @abstractmethod
    def soft_links(self) -> list[dict]: ...

    @abstractmethod
    def update_node_metadata(self, urn: str, **kwargs: Any) -> None: ...

    @abstractmethod
    def delete_node_metadata(self, urn: str, *keys: str) -> None: ...

    @abstractmethod
    def add_soft_link(self, link: dict) -> None: ...

    @abstractmethod
    def remove_soft_link(self, link_id: str) -> None: ...

    @abstractmethod
    def node_dict(self, urn: str) -> dict | None: ...

    @abstractmethod
    def reload(self) -> None: ...

    def stop_watcher(self) -> None:  # noqa: B027
        """Stop a background file-watcher, if any. No-op by default."""
