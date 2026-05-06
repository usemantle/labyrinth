"""Resolver: Fix placeholder security group URNs in ProtectedByEdge entries."""

from __future__ import annotations

import logging
import uuid

from labyrinth.graph.edges.protected_by_edge import ProtectedByEdge
from labyrinth.graph.graph_models import (
    URN,
    Edge,
    EdgeType,
    Graph,
    NodeMetadataKey,
    NodeType,
)
from labyrinth.graph.stitchers._base import Resolver

logger = logging.getLogger(__name__)


class SecurityGroupResolver(Resolver):
    """Replace ProtectedByEdge entries with ':unknown/sg/' placeholder URNs with real SG URNs."""

    def resolve(self, organization_id: uuid.UUID, graph: Graph, context: dict) -> Graph:
        NK = NodeMetadataKey

        sg_by_id: dict[str, URN] = {}
        for node in graph.nodes:
            if node.node_type == NodeType.SECURITY_GROUP:
                sg_id = node.metadata.get(NK.SG_ID, "")
                if sg_id:
                    sg_by_id[sg_id] = node.urn

        if not sg_by_id:
            return graph

        resolved_count = 0
        new_edges: list[Edge] = []
        drop_indices: list[int] = []

        for i, edge in enumerate(graph.edges):
            if edge.edge_type != EdgeType.PROTECTED_BY:
                continue
            to_str = str(edge.to_urn)
            if ":unknown/sg/" not in to_str:
                continue
            sg_id = to_str.rsplit("/sg/", 1)[-1]
            real_urn = sg_by_id.get(sg_id)
            if real_urn:
                new_edges.append(ProtectedByEdge.create(
                    organization_id, edge.from_urn, real_urn,
                    metadata=edge.metadata,
                ))
                drop_indices.append(i)
                resolved_count += 1

        for i in reversed(drop_indices):
            graph.edges.pop(i)
        graph.edges.extend(new_edges)

        if resolved_count:
            logger.info("Resolved %d security group URNs from 'unknown' placeholders", resolved_count)

        return graph
