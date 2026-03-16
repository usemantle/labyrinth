"""Stitcher: ECS_TASK_DEFINITION nodes -> IMAGE_REPOSITORY nodes via ReferencesEdge."""

from __future__ import annotations

import uuid

from src.graph.edges.references_edge import ReferencesEdge
from src.graph.graph_models import (
    EdgeMetadata,
    EdgeMetadataKey,
    Graph,
    NodeMetadataKey,
    NodeType,
)
from src.graph.stitchers._base import Stitcher


class EcsTaskToEcrStitcher(Stitcher):
    """Match ECS task definition container image URIs to ECR repository URIs."""

    def stitch(self, organization_id: uuid.UUID, graph: Graph, context: dict) -> Graph:
        NK = NodeMetadataKey
        result = Graph()

        idx = self.index_nodes(
            graph,
            types={NodeType.IMAGE_REPOSITORY, NodeType.ECS_TASK_DEFINITION},
            metadata_keys={NK.REPOSITORY_URI},
        )

        ecr_by_uri_prefix = idx.lookup(NK.REPOSITORY_URI)
        task_defs = idx.nodes_of_type(NodeType.ECS_TASK_DEFINITION)

        for td_node in task_defs:
            images = td_node.metadata.get(NK.ECS_CONTAINER_IMAGES, [])
            if not isinstance(images, list):
                continue
            for image_uri in images:
                repo_uri = image_uri.split(":")[0] if ":" in image_uri else image_uri
                repo_uri = repo_uri.split("@")[0] if "@" in repo_uri else repo_uri

                ecr_urn = ecr_by_uri_prefix.get(repo_uri)
                if ecr_urn:
                    result.edges.append(ReferencesEdge.create(
                        organization_id, td_node.urn, ecr_urn,
                        metadata=EdgeMetadata({
                            EdgeMetadataKey.DETECTION_METHOD: "image_uri_match",
                            EdgeMetadataKey.CONFIDENCE: 1.0,
                        }),
                    ))

        return result
