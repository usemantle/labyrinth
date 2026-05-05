"""Tests for IdentityCenterToIamStitcher."""

from __future__ import annotations

import uuid

from labyrinth.graph.edges.member_of_edge import MemberOfEdge
from labyrinth.graph.edges.sso_assigned_to_edge import SsoAssignedToEdge
from labyrinth.graph.graph_models import (
    URN,
    EdgeMetadataKey,
    EdgeType,
    Graph,
    NodeMetadataKey,
    NodeType,
)
from labyrinth.graph.nodes.aws.permission_set_node import PermissionSetNode
from labyrinth.graph.nodes.aws.sso_user_node import SsoUserNode
from labyrinth.graph.nodes.iam_role_node import IamRoleNode
from labyrinth.graph.nodes.sso_group_node import SsoGroupNode
from labyrinth.graph.stitchers.identity_center_to_iam import IdentityCenterToIamStitcher

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey
EK = EdgeMetadataKey
SSO_MGMT_ACCOUNT = "123456789012"
INSTANCE_ARN = "arn:aws:sso:::instance/ssoins-test"
PS_ARN = "arn:aws:sso:::permissionSet/ssoins-test/ps-admin"


def _ps(name: str = "AdminAccess", *, ps_id: str = "ps-admin") -> PermissionSetNode:
    return PermissionSetNode.create(
        ORG_ID,
        URN(f"urn:aws:sso:{SSO_MGMT_ACCOUNT}::permission-set/{ps_id}"),
        permission_set_arn=PS_ARN,
        instance_arn=INSTANCE_ARN,
        name=name,
    )


def _sso_user(user_id: str) -> SsoUserNode:
    return SsoUserNode.create(
        ORG_ID,
        URN(f"urn:aws:sso:{SSO_MGMT_ACCOUNT}::user/{user_id}"),
        user_id=user_id,
    )


def _sso_group(group_id: str) -> SsoGroupNode:
    return SsoGroupNode.create(
        ORG_ID,
        URN(f"urn:aws:sso:{SSO_MGMT_ACCOUNT}::group/{group_id}"),
        group_id=group_id,
    )


def _iam_role(account_id: str, role_name: str) -> IamRoleNode:
    return IamRoleNode.create(
        ORG_ID,
        URN(f"urn:aws:iam:{account_id}::role/{role_name}"),
        role_name=role_name,
        arn=f"arn:aws:iam::{account_id}:role/{role_name}",
    )


def _assigned(principal: SsoUserNode | SsoGroupNode, ps: PermissionSetNode,
              account_id: str) -> SsoAssignedToEdge:
    return SsoAssignedToEdge.create(
        ORG_ID,
        from_urn=principal.urn,
        to_urn=ps.urn,
        account_id=account_id,
    )


def _membership(user: SsoUserNode, group: SsoGroupNode) -> MemberOfEdge:
    return MemberOfEdge.create(ORG_ID, from_urn=user.urn, to_urn=group.urn)


class TestIdentityCenterToIam:
    def test_user_assigned_in_one_account_emits_assumes_edge(self):
        ps = _ps("AdminAccess")
        alice = _sso_user("u-alice")
        role = _iam_role("111111111111", "AWSReservedSSO_AdminAccess_abc12345")
        unrelated = _iam_role("111111111111", "MyAppRole")
        graph = Graph(
            nodes=[ps, alice, role, unrelated],
            edges=[_assigned(alice, ps, "111111111111")],
        )

        result = IdentityCenterToIamStitcher().stitch(ORG_ID, graph, {})

        # Permission set -> role
        ps_role = [e for e in result.edges
                   if e.edge_type == EdgeType.ASSUMES
                   and str(e.from_urn) == str(ps.urn)
                   and str(e.to_urn) == str(role.urn)]
        assert len(ps_role) == 1
        assert ps_role[0].metadata[EK.ASSUMED_VIA] == "iam:permission_set_role"

        # User -> role
        user_role = [e for e in result.edges
                     if e.edge_type == EdgeType.ASSUMES
                     and str(e.from_urn) == str(alice.urn)
                     and str(e.to_urn) == str(role.urn)]
        assert len(user_role) == 1
        assert user_role[0].metadata[EK.ASSUMED_VIA] == "sso:permission_set"
        assert user_role[0].metadata[EK.ACCOUNT_ID] == "111111111111"
        assert user_role[0].metadata[EK.PERMISSION_SET_ARN] == PS_ARN

        # Unrelated role gets no edge
        assert not any(str(e.to_urn) == str(unrelated.urn) for e in result.edges)

    def test_group_assignment_fans_out_to_member_users(self):
        ps = _ps("DevAccess", ps_id="ps-dev")
        devs = _sso_group("g-devs")
        bob = _sso_user("u-bob")
        carol = _sso_user("u-carol")
        role = _iam_role("222222222222", "AWSReservedSSO_DevAccess_def67890")
        graph = Graph(
            nodes=[ps, devs, bob, carol, role],
            edges=[
                _membership(bob, devs),
                _membership(carol, devs),
                _assigned(devs, ps, "222222222222"),
            ],
        )

        result = IdentityCenterToIamStitcher().stitch(ORG_ID, graph, {})

        # Group -> role (no via_group on this one)
        group_role = [e for e in result.edges
                      if str(e.from_urn) == str(devs.urn)
                      and str(e.to_urn) == str(role.urn)]
        assert len(group_role) == 1
        assert EK.VIA_GROUP not in group_role[0].metadata

        # Each user fans out to role with via_group
        for user in (bob, carol):
            user_role = [e for e in result.edges
                         if str(e.from_urn) == str(user.urn)
                         and str(e.to_urn) == str(role.urn)]
            assert len(user_role) == 1
            assert user_role[0].metadata[EK.VIA_GROUP] == str(devs.urn)
            assert user_role[0].metadata[EK.ASSUMED_VIA] == "sso:permission_set"

    def test_assignment_in_account_with_no_provisioned_role_yields_no_user_edge(self):
        ps = _ps("AdminAccess")
        alice = _sso_user("u-alice")
        # Role is in account 111, but assignment is to account 222.
        role = _iam_role("111111111111", "AWSReservedSSO_AdminAccess_abc12345")
        graph = Graph(
            nodes=[ps, alice, role],
            edges=[_assigned(alice, ps, "222222222222")],
        )

        result = IdentityCenterToIamStitcher().stitch(ORG_ID, graph, {})

        user_role_edges = [e for e in result.edges
                           if str(e.from_urn) == str(alice.urn)]
        assert len(user_role_edges) == 0

    def test_permission_set_with_spaces_normalises_for_role_match(self):
        ps = _ps("Admin Access With Spaces", ps_id="ps-spaced")
        # AWS strips spaces when generating the role name.
        role = _iam_role("333333333333", "AWSReservedSSO_AdminAccessWithSpaces_abcdef12")
        alice = _sso_user("u-alice")
        graph = Graph(
            nodes=[ps, role, alice],
            edges=[_assigned(alice, ps, "333333333333")],
        )

        result = IdentityCenterToIamStitcher().stitch(ORG_ID, graph, {})

        ps_role = [e for e in result.edges
                   if str(e.from_urn) == str(ps.urn)
                   and str(e.to_urn) == str(role.urn)]
        assert len(ps_role) == 1

    def test_no_permission_sets_returns_empty(self):
        alice = _sso_user("u-alice")
        role = _iam_role("111111111111", "AWSReservedSSO_AdminAccess_abc12345")
        graph = Graph(nodes=[alice, role])

        result = IdentityCenterToIamStitcher().stitch(ORG_ID, graph, {})
        assert len(result.edges) == 0

    def test_no_assignment_emits_only_ps_role_edges(self):
        ps = _ps("AdminAccess")
        role = _iam_role("111111111111", "AWSReservedSSO_AdminAccess_abc12345")
        graph = Graph(nodes=[ps, role])

        result = IdentityCenterToIamStitcher().stitch(ORG_ID, graph, {})

        edge_types = {e.metadata.get(EK.ASSUMED_VIA) for e in result.edges}
        assert edge_types == {"iam:permission_set_role"}
