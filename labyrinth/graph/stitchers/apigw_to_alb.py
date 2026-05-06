"""Stitcher: API Gateway LOAD_BALANCER nodes -> ALB LOAD_BALANCER nodes via RoutesToEdge."""

from __future__ import annotations

import uuid

from labyrinth.graph.edges.routes_to_edge import RoutesToEdge
from labyrinth.graph.graph_models import (
    URN,
    EdgeMetadata,
    EdgeMetadataKey,
    Graph,
    Node,
    NodeMetadataKey,
    NodeType,
)
from labyrinth.graph.stitchers._base import Stitcher


class ApiGwToAlbStitcher(Stitcher):
    """Match API Gateway integration URIs containing listener ARN paths to ALB ARNs."""

    def stitch(self, organization_id: uuid.UUID, graph: Graph, context: dict) -> Graph:
        NK = NodeMetadataKey
        result = Graph()

        idx = self.index_nodes(graph, types={NodeType.LOAD_BALANCER})

        api_gw_nodes: list[Node] = []
        lb_by_listener_arn: dict[str, URN] = {}

        for node in idx.nodes_of_type(NodeType.LOAD_BALANCER):
            if node.metadata.get(NK.API_GW_INTEGRATION_URIS):
                api_gw_nodes.append(node)

            listeners = node.metadata.get(NK.LB_LISTENERS, [])
            lb_arn = node.metadata.get(NK.ARN, "")
            if isinstance(listeners, list) and lb_arn:
                lb_by_listener_arn[lb_arn] = node.urn

        for apigw_node in api_gw_nodes:
            integration_uris = apigw_node.metadata.get(NK.API_GW_INTEGRATION_URIS, [])
            if not isinstance(integration_uris, list):
                continue
            for uri in integration_uris:
                for lb_arn, lb_urn in lb_by_listener_arn.items():
                    lb_marker = "loadbalancer/"
                    lb_idx = lb_arn.find(lb_marker)
                    if lb_idx < 0:
                        continue
                    lb_path = lb_arn[lb_idx + len(lb_marker):]
                    if f"listener/{lb_path}" in uri:
                        result.edges.append(RoutesToEdge.create(
                            organization_id, apigw_node.urn, lb_urn,
                            metadata=EdgeMetadata({
                                EdgeMetadataKey.DETECTION_METHOD: "apigw_integration_match",
                                EdgeMetadataKey.CONFIDENCE: 1.0,
                            }),
                        ))
                        break

        return result
