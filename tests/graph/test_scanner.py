"""Tests for Scanner class."""

from __future__ import annotations

import uuid
from pathlib import Path
from unittest.mock import MagicMock

from src.graph.graph_models import Graph, Node, NodeMetadata, NodeMetadataKey, URN
from src.graph.scanner import Scanner

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey


class TestScannerPostProcess:
    def test_validate_raises_on_duplicate_urns(self):
        """Validate raises RuntimeError when duplicate URNs exist."""
        urn = URN("urn:test:svc:acct:region:path")
        graph = Graph(nodes=[
            Node(organization_id=ORG_ID, urn=urn),
            Node(organization_id=ORG_ID, urn=urn),
        ])

        scanner = Scanner(
            project_name="test",
            project_id=ORG_ID,
            targets=[],
            sink=MagicMock(),
            project_dir=Path("/tmp"),
        )

        import pytest
        with pytest.raises(RuntimeError, match="Duplicate node URN"):
            scanner._validate(graph)

    def test_validate_passes_with_unique_urns(self):
        """Validate succeeds with unique URNs."""
        graph = Graph(nodes=[
            Node(organization_id=ORG_ID, urn=URN("urn:test:svc:acct:region:path1")),
            Node(organization_id=ORG_ID, urn=URN("urn:test:svc:acct:region:path2")),
        ])

        scanner = Scanner(
            project_name="test",
            project_id=ORG_ID,
            targets=[],
            sink=MagicMock(),
            project_dir=Path("/tmp"),
        )
        scanner._validate(graph)  # Should not raise
