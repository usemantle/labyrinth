"""Tests for AwsAccountLoader."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pytest

from src.graph.graph_models import URN, NodeMetadataKey
from src.graph.loaders.aws.loader import AwsAccountLoader
from src.graph.loaders.aws.plugins._base import AwsResourcePlugin

ORG_ID = uuid.uuid4()
NK = NodeMetadataKey


class _DummyPlugin(AwsResourcePlugin):
    """Minimal plugin for testing the loader harness."""

    def service_name(self) -> str:
        return "dummy"

    def discover(self, session, account_id, region, organization_id, account_urn, build_urn):
        from src.graph.nodes.bucket_node import BucketNode
        urn = URN(f"urn:aws:s3:{account_id}:{region}:test-bucket")
        node = BucketNode.create(
            organization_id=organization_id,
            urn=urn,
            parent_urn=account_urn,
            bucket_name="test-bucket",
        )
        return [node], []


class TestAwsAccountLoaderBasics:
    def test_display_name(self):
        assert AwsAccountLoader.display_name() == "AWS Account"

    def test_credential_type(self):
        from src.graph.credentials import AWSProfileCredential
        assert AwsAccountLoader.credential_type() is AWSProfileCredential

    def test_build_target_urn(self):
        urn = AwsAccountLoader.build_target_urn(
            account_id="123456789012", region="us-west-2",
        )
        assert str(urn) == "urn:aws:account:123456789012:us-west-2:root"

    def test_available_plugins(self):
        plugins = AwsAccountLoader.available_plugins()
        assert "s3" in plugins
        assert "rds" in plugins
        assert "ecr" in plugins
        assert "ecs" in plugins
        assert "vpc" in plugins
        assert "iam" in plugins
        assert "sso" in plugins

    def test_urn_components(self):
        components = AwsAccountLoader.urn_components()
        names = [c.name for c in components]
        assert "account_id" in names
        assert "region" in names


class TestAwsAccountLoaderLoad:
    def test_load_creates_account_node(self):
        session = MagicMock()
        loader = AwsAccountLoader(
            organization_id=ORG_ID,
            account_id="123456789012",
            region="us-east-1",
            session=session,
        )
        nodes, edges = loader.load("root")
        assert len(nodes) == 1
        assert nodes[0].node_type == "aws_account"
        assert nodes[0].metadata[NK.ACCOUNT_ID] == "123456789012"

    def test_load_with_plugin(self):
        session = MagicMock()
        loader = AwsAccountLoader(
            organization_id=ORG_ID,
            account_id="123456789012",
            region="us-east-1",
            session=session,
            plugins=[_DummyPlugin()],
        )
        nodes, edges = loader.load("root")
        # Account node + bucket from plugin
        assert len(nodes) == 2
        assert nodes[0].node_type == "aws_account"
        assert nodes[1].node_type == "s3_bucket"
        # Should have a ContainsEdge from account to bucket
        contains_edges = [e for e in edges if e.edge_type == "contains"]
        assert len(contains_edges) == 1

    def test_load_plugin_exception_does_not_crash(self):
        """A failing plugin should not crash the entire scan."""

        class _FailingPlugin(AwsResourcePlugin):
            def service_name(self):
                return "failing"

            def discover(self, *args, **kwargs):
                raise RuntimeError("boom")

        session = MagicMock()
        loader = AwsAccountLoader(
            organization_id=ORG_ID,
            account_id="123456789012",
            region="us-east-1",
            session=session,
            plugins=[_FailingPlugin(), _DummyPlugin()],
        )
        nodes, edges = loader.load("root")
        # Should still have account node + bucket from non-failing plugin
        assert len(nodes) == 2

    def test_build_urn(self):
        session = MagicMock()
        loader = AwsAccountLoader(
            organization_id=ORG_ID,
            account_id="123456789012",
            region="us-east-1",
            session=session,
        )
        urn = loader.build_urn("s3", "my-bucket")
        assert str(urn) == "urn:aws:account:123456789012:us-east-1:s3/my-bucket"


class TestAwsAccountLoaderFromConfig:
    @patch("src.graph.loaders.aws.boto3")
    def test_from_target_config_with_profile(self, mock_boto3):
        mock_session = MagicMock()
        mock_boto3.Session.return_value = mock_session

        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_session.client.return_value = mock_sts

        urn = URN("urn:aws:account:123456789012:us-east-1:root")
        credentials = {"type": "aws_profile", "profile": "prod"}

        loader, resource = AwsAccountLoader.from_target_config(
            ORG_ID, urn, credentials,
        )
        assert isinstance(loader, AwsAccountLoader)
        assert resource == "root"
        mock_boto3.Session.assert_called_once_with(
            profile_name="prod", region_name="us-east-1",
        )

    @patch("src.graph.loaders.aws.boto3")
    def test_from_target_config_with_temp_credentials(self, mock_boto3):
        mock_session = MagicMock()
        mock_boto3.Session.return_value = mock_session

        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_session.client.return_value = mock_sts

        urn = URN("urn:aws:account:123456789012:us-east-1:root")
        credentials = {
            "type": "aws_profile",
            "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
            "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "aws_session_token": "FwoGZXIvYXdzEBY...",
        }

        loader, resource = AwsAccountLoader.from_target_config(
            ORG_ID, urn, credentials,
        )
        assert isinstance(loader, AwsAccountLoader)
        mock_boto3.Session.assert_called_once_with(
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            aws_session_token="FwoGZXIvYXdzEBY...",
            region_name="us-east-1",
        )
