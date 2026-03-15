"""Tests for SsoResourcePlugin."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from src.graph.graph_models import URN, NodeMetadataKey
from src.graph.loaders.aws.plugins.sso_plugin import SsoResourcePlugin

ORG_ID = uuid.uuid4()
NK = NodeMetadataKey
ACCOUNT_URN = URN("urn:aws:account:123456789012:us-east-1:root")


class TestSsoResourcePlugin:
    def test_service_name(self):
        assert SsoResourcePlugin().service_name() == "sso"

    def test_discover_groups(self):
        session = MagicMock()
        sso_admin = MagicMock()
        identity_store = MagicMock()

        def client_factory(service, **kwargs):
            if service == "sso-admin":
                return sso_admin
            if service == "identitystore":
                return identity_store
            return MagicMock()

        session.client = client_factory
        sso_admin.list_instances.return_value = {
            "Instances": [{"IdentityStoreId": "d-123"}],
        }

        group_paginator = MagicMock()
        group_paginator.paginate.return_value = [{
            "Groups": [
                {"GroupId": "g-abc", "DisplayName": "Developers"},
                {"GroupId": "g-def", "DisplayName": "Admins"},
            ],
        }]
        identity_store.get_paginator.return_value = group_paginator

        plugin = SsoResourcePlugin()
        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 2
        assert all(n.node_type == "sso_group" for n in nodes)
        names = {n.metadata[NK.SSO_GROUP_NAME] for n in nodes}
        assert names == {"Developers", "Admins"}

    def test_discover_no_sso_instance(self):
        session = MagicMock()
        sso_admin = MagicMock()

        def client_factory(service, **kwargs):
            if service == "sso-admin":
                return sso_admin
            return MagicMock()

        session.client = client_factory
        sso_admin.list_instances.return_value = {"Instances": []}

        plugin = SsoResourcePlugin()
        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 0
