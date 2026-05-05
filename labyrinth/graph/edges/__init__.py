"""Typed edge subclasses for the security graph."""

from labyrinth.graph.edges._base import validate_edge
from labyrinth.graph.edges.allows_traffic_to_edge import AllowsTrafficToEdge
from labyrinth.graph.edges.assumes_edge import AssumesEdge
from labyrinth.graph.edges.attaches_edge import AttachesEdge
from labyrinth.graph.edges.builds_edge import BuildsEdge
from labyrinth.graph.edges.calls_edge import CallsEdge
from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.depends_on_edge import DependsOnEdge
from labyrinth.graph.edges.executes_edge import ExecutesEdge
from labyrinth.graph.edges.hosts_edge import HostsEdge
from labyrinth.graph.edges.instantiates_edge import InstantiatesEdge
from labyrinth.graph.edges.member_of_edge import MemberOfEdge
from labyrinth.graph.edges.models_edge import ModelsEdge
from labyrinth.graph.edges.protected_by_edge import ProtectedByEdge
from labyrinth.graph.edges.reads_edge import ReadsEdge
from labyrinth.graph.edges.references_edge import ReferencesEdge
from labyrinth.graph.edges.resolves_to_edge import ResolvesToEdge
from labyrinth.graph.edges.routes_to_edge import RoutesToEdge
from labyrinth.graph.edges.soft_reference_edge import SoftReferenceEdge
from labyrinth.graph.edges.writes_edge import WritesEdge

__all__ = [
    "AllowsTrafficToEdge",
    "AssumesEdge",
    "AttachesEdge",
    "BuildsEdge",
    "CallsEdge",
    "ContainsEdge",
    "DependsOnEdge",
    "ExecutesEdge",
    "HostsEdge",
    "InstantiatesEdge",
    "MemberOfEdge",
    "ModelsEdge",
    "ProtectedByEdge",
    "ReadsEdge",
    "ReferencesEdge",
    "ResolvesToEdge",
    "RoutesToEdge",
    "SoftReferenceEdge",
    "WritesEdge",
    "validate_edge",
]
