from __future__ import annotations

from typing import Any

from src.graph.graph_models import Edge, Node
from src.graph.sinks import Sink


class SqliteSink(Sink):
    """Write graph data to a SQLite database (stub)."""

    def __init__(self, _: str):
        # TODO: pass db identifier
        pass

    def write(self, nodes: list[Node], edges: list[Edge]) -> None:
        pass

    def update_node_metadata(self, urn: str, **kwargs: Any) -> None:
        pass

    def delete_node_metadata(self, urn: str, *keys: str) -> None:
        pass

    def add_soft_link(self, link: dict) -> None:
        pass

    def remove_soft_link(self, link_id: str) -> None:
        pass
