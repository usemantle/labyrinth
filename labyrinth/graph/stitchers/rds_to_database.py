"""Stitcher: RDS_CLUSTER nodes -> DATABASE nodes via HostsEdge."""

from __future__ import annotations

import uuid

from labyrinth.graph.edges.hosts_edge import HostsEdge
from labyrinth.graph.graph_models import (
    EdgeMetadata,
    EdgeMetadataKey,
    Graph,
    NodeMetadataKey,
    NodeType,
)
from labyrinth.graph.stitchers._base import Stitcher


class RdsToDatabaseStitcher(Stitcher):
    """Match RDS cluster endpoints to database hosts."""

    def stitch(self, organization_id: uuid.UUID, graph: Graph, context: dict) -> Graph:
        NK = NodeMetadataKey
        result = Graph()

        idx = self.index_nodes(
            graph,
            types={NodeType.RDS_CLUSTER, NodeType.DATABASE},
            metadata_keys={NK.RDS_ENDPOINT, NK.HOST},
        )

        rds_by_endpoint = idx.lookup(NK.RDS_ENDPOINT)
        databases_by_host = idx.lookup(NK.HOST)

        for endpoint, rds_urn in rds_by_endpoint.items():
            db_urn = databases_by_host.get(endpoint)
            if db_urn:
                result.edges.append(HostsEdge.create(
                    organization_id, rds_urn, db_urn,
                    metadata=EdgeMetadata({
                        EdgeMetadataKey.DETECTION_METHOD: "endpoint_match",
                        EdgeMetadataKey.CONFIDENCE: 1.0,
                    }),
                ))

        return result
