"""Typed edge subclasses for the security graph."""

from src.graph.edges._base import validate_edge
from src.graph.edges.calls_edge import CallsEdge
from src.graph.edges.contains_edge import ContainsEdge
from src.graph.edges.depends_on_edge import DependsOnEdge
from src.graph.edges.hosts_edge import HostsEdge
from src.graph.edges.models_edge import ModelsEdge
from src.graph.edges.reads_edge import ReadsEdge
from src.graph.edges.references_edge import ReferencesEdge
from src.graph.edges.soft_reference_edge import SoftReferenceEdge
from src.graph.edges.writes_edge import WritesEdge

__all__ = [
    "CallsEdge",
    "ContainsEdge",
    "DependsOnEdge",
    "HostsEdge",
    "ModelsEdge",
    "ReadsEdge",
    "ReferencesEdge",
    "SoftReferenceEdge",
    "WritesEdge",
    "validate_edge",
]
