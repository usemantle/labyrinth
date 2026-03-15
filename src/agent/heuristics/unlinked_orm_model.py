"""Heuristic: ORM classes with orm_table metadata but no `models` edge to a table."""

from __future__ import annotations

from src.agent.heuristics._base import BaseHeuristic
from src.graph.graph_models import EdgeType, NodeType


class UnlinkedOrmModel(BaseHeuristic):
    name = "unlinked_orm_model"
    source_node_type = NodeType.CLASS
    metadata_key = "orm_table"
    target_edge_type = EdgeType.MODELS
    target_node_type = NodeType.TABLE

    @classmethod
    def get_instructions(cls) -> str:
        return (
            "This ORM class has an orm_table metadata field but no outgoing `models` edge "
            "to a table node. Investigate whether a matching table exists in the graph and "
            "create a `models` edge if evidence supports it."
        )
