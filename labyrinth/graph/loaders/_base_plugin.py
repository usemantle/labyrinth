"""Unified plugin contract shared by all loader domains.

A plugin is scoped to a single domain (one cloud account, one codebase, …)
and is instantiated fresh per scan with the runtime context it needs as
constructor arguments. Plugins expose two hooks:

* ``discover()`` — emit *new* nodes and edges for the domain.
* ``enrich(nodes, edges)`` — mutate the existing graph in place
  (metadata stamps, etc.). Sees nodes produced by earlier ``discover``
  calls in the same pipeline pass.

Both default to no-ops; subclasses override only what they need.
"""

from __future__ import annotations

from labyrinth.graph.graph_models import Edge, Node


class BasePlugin:
    """Base class for every loader plugin."""

    def discover(self) -> tuple[list[Node], list[Edge]]:
        return [], []

    def enrich(self, nodes: list[Node], edges: list[Edge]) -> None:
        return None
