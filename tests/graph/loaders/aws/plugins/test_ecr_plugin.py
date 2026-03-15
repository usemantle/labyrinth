"""Tests for EcrResourcePlugin."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from src.graph.graph_models import URN, NodeMetadataKey
from src.graph.loaders.aws.plugins.ecr_plugin import EcrResourcePlugin

ORG_ID = uuid.uuid4()
NK = NodeMetadataKey
ACCOUNT_URN = URN("urn:aws:account:123456789012:us-east-1:root")


def _make_session(repos):
    session = MagicMock()
    ecr = MagicMock()
    session.client.return_value = ecr

    paginator = MagicMock()
    ecr.get_paginator.return_value = paginator
    paginator.paginate.return_value = [{"repositories": repos}]

    return session


class TestEcrResourcePlugin:
    def test_service_name(self):
        assert EcrResourcePlugin().service_name() == "ecr"

    def test_discover_repos(self):
        repos = [
            {
                "repositoryName": "my-app",
                "repositoryUri": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-app",
                "repositoryArn": "arn:aws:ecr:us-east-1:123456789012:repository/my-app",
            },
        ]
        session = _make_session(repos)
        plugin = EcrResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 1
        assert nodes[0].node_type == "image_repository"
        assert nodes[0].metadata[NK.REPOSITORY_NAME] == "my-app"
        assert nodes[0].parent_urn == ACCOUNT_URN

    def test_discover_empty(self):
        session = _make_session([])
        plugin = EcrResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 0
