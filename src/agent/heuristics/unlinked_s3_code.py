"""Heuristic: functions with S3 operations but no reads/writes edge to a bucket."""

from __future__ import annotations

from src.agent.heuristics._base import BaseHeuristic
from src.graph.graph_models import NodeType


class UnlinkedS3Code(BaseHeuristic):
    name = "unlinked_s3_code"
    source_node_type = NodeType.FUNCTION
    metadata_key = "aws_s3_operations"
    skill_file = "link-code-to-s3.md"

    @classmethod
    def get_instructions(cls) -> str:
        return (
            "This function has S3 operations (aws_s3_operations metadata) but no outgoing "
            "`reads` or `writes` edge to an S3 bucket node. Investigate which S3 bucket "
            "this function operates on. Follow the investigation steps in the skill file."
        )
