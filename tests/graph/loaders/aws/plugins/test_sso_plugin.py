"""Tests for SsoResourcePlugin (groups + users + memberships)."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from src.graph.graph_models import URN, EdgeType, NodeMetadataKey
from src.graph.loaders.aws.plugins.sso_plugin import SsoResourcePlugin

ORG_ID = uuid.uuid4()
NK = NodeMetadataKey
ACCOUNT_URN = URN("urn:aws:account:123456789012:us-east-1:root")


def _build_session(*, groups: list[dict] | None = None, users: list[dict] | None = None,
                   memberships_by_group: dict[str, list[dict]] | None = None,
                   instances: list[dict] | None = None):
    """Build a MagicMock boto3 session whose identitystore paginator dispatches by name."""
    session = MagicMock()
    sso_admin = MagicMock()
    identity_store = MagicMock()

    sso_admin.list_instances.return_value = {
        "Instances": (
            instances if instances is not None else [{"IdentityStoreId": "d-123"}]
        ),
    }

    paginators: dict[str, MagicMock] = {}
    for name, data_key, items in [
        ("list_groups", "Groups", groups or []),
        ("list_users", "Users", users or []),
    ]:
        p = MagicMock()
        p.paginate.return_value = [{data_key: items}]
        paginators[name] = p

    membership_paginator = MagicMock()

    def membership_paginate(IdentityStoreId, GroupId, **kwargs):  # noqa: N803
        members = (memberships_by_group or {}).get(GroupId, [])
        return [{"GroupMemberships": members}]

    membership_paginator.paginate.side_effect = membership_paginate
    paginators["list_group_memberships"] = membership_paginator

    identity_store.get_paginator.side_effect = lambda name: paginators[name]

    def client_factory(service, **kwargs):
        if service == "sso-admin":
            return sso_admin
        if service == "identitystore":
            return identity_store
        return MagicMock()

    session.client = client_factory
    return session


class TestSsoResourcePlugin:
    def test_service_name(self):
        assert SsoResourcePlugin().service_name() == "sso"

    def test_discover_groups(self):
        session = _build_session(groups=[
            {"GroupId": "g-abc", "DisplayName": "Developers"},
            {"GroupId": "g-def", "DisplayName": "Admins"},
        ])
        plugin = SsoResourcePlugin()
        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )
        groups = [n for n in nodes if n.node_type == "sso_group"]
        assert {n.metadata[NK.SSO_GROUP_NAME] for n in groups} == {"Developers", "Admins"}

    def test_discover_users(self):
        session = _build_session(users=[
            {
                "UserId": "u-abc",
                "UserName": "alice",
                "Emails": [
                    {"Value": "alt@example.com", "Primary": False},
                    {"Value": "alice@example.com", "Primary": True},
                ],
                "ExternalIds": [{"Issuer": "okta", "Id": "00u1"}],
            },
        ])
        plugin = SsoResourcePlugin()
        nodes, _ = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )
        users = [n for n in nodes if n.node_type == "sso_user"]
        assert len(users) == 1
        u = users[0]
        assert u.metadata[NK.SSO_USER_ID] == "u-abc"
        assert u.metadata[NK.SSO_USER_NAME] == "alice"
        assert u.metadata[NK.SSO_USER_EMAIL] == "alice@example.com"
        assert u.metadata[NK.SSO_USER_EXTERNAL_ID] == "00u1"

    def test_discover_group_memberships(self):
        session = _build_session(
            groups=[{"GroupId": "g-abc", "DisplayName": "Devs"}],
            users=[
                {"UserId": "u-1", "UserName": "alice"},
                {"UserId": "u-2", "UserName": "bob"},
            ],
            memberships_by_group={
                "g-abc": [
                    {"MemberId": {"UserId": "u-1"}},
                    {"MemberId": {"UserId": "u-2"}},
                ],
            },
        )
        plugin = SsoResourcePlugin()
        _, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )
        member_of = [e for e in edges if e.edge_type == EdgeType.MEMBER_OF]
        assert len(member_of) == 2
        froms = {str(e.from_urn) for e in member_of}
        assert froms == {
            "urn:aws:sso:123456789012::user/u-1",
            "urn:aws:sso:123456789012::user/u-2",
        }
        assert all(str(e.to_urn) == "urn:aws:sso:123456789012::group/g-abc" for e in member_of)

    def test_user_without_primary_email_falls_back_to_first(self):
        session = _build_session(users=[{
            "UserId": "u-x", "UserName": "x",
            "Emails": [{"Value": "first@example.com"}, {"Value": "second@example.com"}],
        }])
        plugin = SsoResourcePlugin()
        nodes, _ = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )
        u = next(n for n in nodes if n.node_type == "sso_user")
        assert u.metadata[NK.SSO_USER_EMAIL] == "first@example.com"

    def test_membership_with_unknown_user_skipped(self):
        session = _build_session(
            groups=[{"GroupId": "g-abc", "DisplayName": "Devs"}],
            users=[],  # no users known
            memberships_by_group={"g-abc": [{"MemberId": {"UserId": "u-orphan"}}]},
        )
        plugin = SsoResourcePlugin()
        _, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )
        assert all(e.edge_type != EdgeType.MEMBER_OF for e in edges)

    def test_discover_no_sso_instance(self):
        session = _build_session(instances=[])
        plugin = SsoResourcePlugin()
        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )
        assert len(nodes) == 0
        assert len(edges) == 0
