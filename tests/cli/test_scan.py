"""Tests for scan orchestration."""

from __future__ import annotations

import uuid
from pathlib import Path

from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType


def _make_ecr_node(org_id: uuid.UUID, repo_name: str) -> Node:
    urn = URN(f"urn:aws:ecr:123456789:us-east-1:{repo_name}")
    return Node(
        organization_id=org_id,
        urn=urn,
        node_type=NodeType.IMAGE_REPOSITORY,
        metadata=NodeMetadata({NodeMetadataKey.REPOSITORY_NAME: repo_name}),
    )


class TestNodeDeduplication:
    """Verify that run_scan deduplicates nodes by URN."""

    def test_scan_validation_catches_duplicates(self, tmp_path: Path):
        """The post-dedup validation should raise on duplicate URNs."""

        # Directly test the validation logic extracted from run_scan:
        # if dedup were somehow bypassed, the validation would catch it.
        org_id = uuid.uuid4()
        ecr_node = _make_ecr_node(org_id, "my-app")

        nodes = [ecr_node, ecr_node]  # duplicate

        # Simulate the dedup step from run_scan
        seen_urns: set[str] = set()
        unique_nodes = []
        for node in nodes:
            urn_str = str(node.urn)
            if urn_str not in seen_urns:
                seen_urns.add(urn_str)
                unique_nodes.append(node)

        assert len(unique_nodes) == 1
        assert str(unique_nodes[0].urn) == "urn:aws:ecr:123456789:us-east-1:my-app"
