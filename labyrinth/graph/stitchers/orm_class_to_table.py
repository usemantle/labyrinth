"""Stitcher: ORM CLASS nodes -> TABLE nodes via ModelsEdge."""

from __future__ import annotations

import uuid

from labyrinth.graph.edges.models_edge import ModelsEdge
from labyrinth.graph.graph_models import (
    URN,
    EdgeMetadata,
    EdgeMetadataKey,
    Graph,
    NodeMetadataKey,
)
from labyrinth.graph.stitchers._base import Stitcher


class OrmClassToTableStitcher(Stitcher):
    """Link ORM class nodes to their corresponding database table nodes."""

    def stitch(self, organization_id: uuid.UUID, graph: Graph, context: dict) -> Graph:
        NK = NodeMetadataKey
        result = Graph()

        idx = self.index_nodes(graph, metadata_keys={NK.TABLE_NAME, NK.ORM_TABLE, NK.CLASS_NAME})

        table_registry = idx.lookup(NK.TABLE_NAME)
        if not table_registry:
            return result

        # Build ORM registry: class_name -> (class_urn, table_name)
        orm_registry: dict[str, tuple[URN, str]] = {}
        for node in graph.nodes:
            if NK.ORM_TABLE in node.metadata:
                orm_registry[node.metadata[NK.CLASS_NAME]] = (
                    node.urn,
                    node.metadata[NK.ORM_TABLE],
                )

        if not orm_registry:
            return result

        for class_name, (class_urn, table_name) in orm_registry.items():
            if table_name in table_registry:
                table_urn = table_registry[table_name]
                result.edges.append(ModelsEdge.create(
                    organization_id,
                    class_urn,
                    table_urn,
                    metadata=EdgeMetadata({
                        EdgeMetadataKey.DETECTION_METHOD: "orm_tablename",
                        EdgeMetadataKey.CONFIDENCE: 1.0,
                        EdgeMetadataKey.ORM_FRAMEWORK: "sqlalchemy",
                        EdgeMetadataKey.ORM_CLASS: class_name,
                        EdgeMetadataKey.TABLE_NAME: table_name,
                    }),
                ))

        return result
