"""Tests for SsoResourcePlugin (groups + users + memberships + permission sets)."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from src.graph.graph_models import URN, EdgeMetadataKey, EdgeType, NodeMetadataKey, NodeType
from src.graph.loaders.aws.plugins.sso_plugin import SsoResourcePlugin

ORG_ID = uuid.uuid4()
NK = NodeMetadataKey
EK = EdgeMetadataKey
ACCOUNT_URN = URN("urn:aws:account:123456789012:us-east-1:root")
DEFAULT_INSTANCE_ARN = "arn:aws:sso:::instance/ssoins-1234567890abcdef"


def _build_session(
    *,
    groups: list[dict] | None = None,
    users: list[dict] | None = None,
    memberships_by_group: dict[str, list[dict]] | None = None,
    instances: list[dict] | None = None,
    permission_sets: list[str] | None = None,
    permission_set_descriptions: dict[str, dict] | None = None,
    accounts_by_permission_set: dict[str, list[str]] | None = None,
    assignments_by_ps_account: dict[tuple[str, str], list[dict]] | None = None,
):
    """Build a MagicMock boto3 session whose paginators dispatch by operation name."""
    session = MagicMock()
    sso_admin = MagicMock()
    identity_store = MagicMock()

    sso_admin.list_instances.return_value = {
        "Instances": (
            instances if instances is not None else [{
                "IdentityStoreId": "d-123",
                "InstanceArn": DEFAULT_INSTANCE_ARN,
            }]
        ),
    }

    def describe_ps(InstanceArn, PermissionSetArn, **kwargs):  # noqa: N803
        described = (permission_set_descriptions or {}).get(PermissionSetArn, {})
        ps_id = PermissionSetArn.rsplit("/", 1)[-1]
        return {"PermissionSet": {"Name": described.get("Name", ps_id), **described}}

    sso_admin.describe_permission_set.side_effect = describe_ps

    identity_paginators: dict[str, MagicMock] = {}
    for name, data_key, items in [
        ("list_groups", "Groups", groups or []),
        ("list_users", "Users", users or []),
    ]:
        p = MagicMock()
        p.paginate.return_value = [{data_key: items}]
        identity_paginators[name] = p

    membership_paginator = MagicMock()

    def membership_paginate(IdentityStoreId, GroupId, **kwargs):  # noqa: N803
        members = (memberships_by_group or {}).get(GroupId, [])
        return [{"GroupMemberships": members}]

    membership_paginator.paginate.side_effect = membership_paginate
    identity_paginators["list_group_memberships"] = membership_paginator

    identity_store.get_paginator.side_effect = lambda name: identity_paginators[name]

    sso_admin_paginators: dict[str, MagicMock] = {}

    list_ps_paginator = MagicMock()
    list_ps_paginator.paginate.return_value = [{"PermissionSets": permission_sets or []}]
    sso_admin_paginators["list_permission_sets"] = list_ps_paginator

    accounts_paginator = MagicMock()

    def accounts_paginate(InstanceArn, PermissionSetArn, **kwargs):  # noqa: N803
        accounts = (accounts_by_permission_set or {}).get(PermissionSetArn, [])
        return [{"AccountIds": accounts}]

    accounts_paginator.paginate.side_effect = accounts_paginate
    sso_admin_paginators["list_accounts_for_provisioned_permission_set"] = accounts_paginator

    assignments_paginator = MagicMock()

    def assignments_paginate(InstanceArn, AccountId, PermissionSetArn, **kwargs):  # noqa: N803
        rows = (assignments_by_ps_account or {}).get((PermissionSetArn, AccountId), [])
        return [{"AccountAssignments": rows}]

    assignments_paginator.paginate.side_effect = assignments_paginate
    sso_admin_paginators["list_account_assignments"] = assignments_paginator

    sso_admin.get_paginator.side_effect = lambda name: sso_admin_paginators[name]

    def client_factory(service, **kwargs):
        if service == "sso-admin":
            return sso_admin
        if service == "identitystore":
            return identity_store
        return MagicMock()

    session.client = client_factory
    return session


def _discover(session) -> tuple[list, list]:
    plugin = SsoResourcePlugin()
    return plugin.discover(
        session=session,
        account_id="123456789012",
        region="us-east-1",
        organization_id=ORG_ID,
        account_urn=ACCOUNT_URN,
        build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
    )


class TestSsoResourcePlugin:
    def test_service_name(self):
        assert SsoResourcePlugin().service_name() == "sso"

    def test_discover_groups(self):
        session = _build_session(groups=[
            {"GroupId": "g-abc", "DisplayName": "Developers"},
            {"GroupId": "g-def", "DisplayName": "Admins"},
        ])
        nodes, _ = _discover(session)
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
        nodes, _ = _discover(session)
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
        _, edges = _discover(session)
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
        nodes, _ = _discover(session)
        u = next(n for n in nodes if n.node_type == "sso_user")
        assert u.metadata[NK.SSO_USER_EMAIL] == "first@example.com"

    def test_membership_with_unknown_user_skipped(self):
        session = _build_session(
            groups=[{"GroupId": "g-abc", "DisplayName": "Devs"}],
            users=[],
            memberships_by_group={"g-abc": [{"MemberId": {"UserId": "u-orphan"}}]},
        )
        _, edges = _discover(session)
        assert all(e.edge_type != EdgeType.MEMBER_OF for e in edges)

    def test_discover_no_sso_instance(self):
        session = _build_session(instances=[])
        nodes, edges = _discover(session)
        assert len(nodes) == 0
        assert len(edges) == 0

    def test_discover_permission_sets(self):
        ps_arn = (
            "arn:aws:sso:::permissionSet/ssoins-1234567890abcdef/ps-abc123"
        )
        session = _build_session(
            permission_sets=[ps_arn],
            permission_set_descriptions={
                ps_arn: {
                    "Name": "AdminAccess",
                    "Description": "Full admin",
                    "SessionDuration": "PT8H",
                },
            },
        )
        nodes, _ = _discover(session)
        ps_nodes = [n for n in nodes if n.node_type == NodeType.PERMISSION_SET]
        assert len(ps_nodes) == 1
        ps = ps_nodes[0]
        assert ps.metadata[NK.PERMISSION_SET_NAME] == "AdminAccess"
        assert ps.metadata[NK.PERMISSION_SET_ARN] == ps_arn
        assert ps.metadata[NK.PERMISSION_SET_INSTANCE_ARN] == DEFAULT_INSTANCE_ARN
        assert ps.metadata[NK.PERMISSION_SET_DESCRIPTION] == "Full admin"
        assert ps.metadata[NK.PERMISSION_SET_SESSION_DURATION] == "PT8H"
        assert str(ps.urn) == "urn:aws:sso:123456789012::permission-set/ps-abc123"

    def test_discover_user_assignment_edge(self):
        ps_arn = (
            "arn:aws:sso:::permissionSet/ssoins-1234567890abcdef/ps-abc123"
        )
        session = _build_session(
            users=[{"UserId": "u-1", "UserName": "alice"}],
            permission_sets=[ps_arn],
            permission_set_descriptions={ps_arn: {"Name": "AdminAccess"}},
            accounts_by_permission_set={ps_arn: ["111111111111", "222222222222"]},
            assignments_by_ps_account={
                (ps_arn, "111111111111"): [
                    {"PrincipalType": "USER", "PrincipalId": "u-1",
                     "AccountId": "111111111111", "PermissionSetArn": ps_arn},
                ],
                (ps_arn, "222222222222"): [
                    {"PrincipalType": "USER", "PrincipalId": "u-1",
                     "AccountId": "222222222222", "PermissionSetArn": ps_arn},
                ],
            },
        )
        _, edges = _discover(session)
        assigned = [e for e in edges if e.edge_type == EdgeType.SSO_ASSIGNED_TO]
        assert len(assigned) == 2
        accounts = {e.metadata["account_id"] for e in assigned}
        assert accounts == {"111111111111", "222222222222"}
        assert all(
            str(e.from_urn) == "urn:aws:sso:123456789012::user/u-1" for e in assigned
        )
        assert all(
            str(e.to_urn) == "urn:aws:sso:123456789012::permission-set/ps-abc123"
            for e in assigned
        )

    def test_discover_group_assignment_edge(self):
        ps_arn = (
            "arn:aws:sso:::permissionSet/ssoins-1234567890abcdef/ps-grp"
        )
        session = _build_session(
            groups=[{"GroupId": "g-1", "DisplayName": "Devs"}],
            permission_sets=[ps_arn],
            permission_set_descriptions={ps_arn: {"Name": "DevAccess"}},
            accounts_by_permission_set={ps_arn: ["111111111111"]},
            assignments_by_ps_account={
                (ps_arn, "111111111111"): [
                    {"PrincipalType": "GROUP", "PrincipalId": "g-1",
                     "AccountId": "111111111111", "PermissionSetArn": ps_arn},
                ],
            },
        )
        _, edges = _discover(session)
        assigned = [e for e in edges if e.edge_type == EdgeType.SSO_ASSIGNED_TO]
        assert len(assigned) == 1
        edge = assigned[0]
        assert str(edge.from_urn) == "urn:aws:sso:123456789012::group/g-1"
        assert edge.metadata["account_id"] == "111111111111"

    def test_assignment_with_unknown_principal_skipped(self):
        ps_arn = (
            "arn:aws:sso:::permissionSet/ssoins-1234567890abcdef/ps-x"
        )
        session = _build_session(
            users=[],
            groups=[],
            permission_sets=[ps_arn],
            permission_set_descriptions={ps_arn: {"Name": "Lonely"}},
            accounts_by_permission_set={ps_arn: ["111111111111"]},
            assignments_by_ps_account={
                (ps_arn, "111111111111"): [
                    {"PrincipalType": "USER", "PrincipalId": "u-orphan",
                     "AccountId": "111111111111", "PermissionSetArn": ps_arn},
                ],
            },
        )
        _, edges = _discover(session)
        assert not any(e.edge_type == EdgeType.SSO_ASSIGNED_TO for e in edges)
