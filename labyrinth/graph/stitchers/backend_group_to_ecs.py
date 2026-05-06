"""Stitcher: BACKEND_GROUP nodes -> ECS_SERVICE nodes via RoutesToEdge."""

from __future__ import annotations

import uuid

from labyrinth.graph.edges.routes_to_edge import RoutesToEdge
from labyrinth.graph.graph_models import (
    URN,
    EdgeMetadata,
    EdgeMetadataKey,
    Graph,
    NodeMetadataKey,
    NodeType,
)
from labyrinth.graph.stitchers._base import Stitcher


class BackendGroupToEcsStitcher(Stitcher):
    """Match target group ARNs from ECS service loadBalancers config."""

    def stitch(self, organization_id: uuid.UUID, graph: Graph, context: dict) -> Graph:
        NK = NodeMetadataKey
        result = Graph()

        idx = self.index_nodes(
            graph,
            types={NodeType.BACKEND_GROUP, NodeType.ECS_SERVICE},
            metadata_keys={NK.ARN},
        )

        bg_by_arn: dict[str, URN] = {}
        for node in idx.nodes_of_type(NodeType.BACKEND_GROUP):
            arn = node.metadata.get(NK.ARN, "")
            if arn:
                bg_by_arn[arn] = node.urn

        for svc_node in idx.nodes_of_type(NodeType.ECS_SERVICE):
            tg_arns = svc_node.metadata.get(NK.ECS_TARGET_GROUP_ARNS, [])
            if not isinstance(tg_arns, list):
                continue
            for tg_arn in tg_arns:
                bg_urn = bg_by_arn.get(tg_arn)
                if bg_urn:
                    result.edges.append(RoutesToEdge.create(
                        organization_id, bg_urn, svc_node.urn,
                        metadata=EdgeMetadata({
                            EdgeMetadataKey.DETECTION_METHOD: "ecs_target_group_match",
                            EdgeMetadataKey.CONFIDENCE: 1.0,
                        }),
                    ))

        return result
