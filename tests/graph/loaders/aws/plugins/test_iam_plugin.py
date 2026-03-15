"""Tests for IamResourcePlugin."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from src.graph.graph_models import URN, NodeMetadataKey
from src.graph.loaders.aws.plugins.iam_plugin import IamResourcePlugin

ORG_ID = uuid.uuid4()
NK = NodeMetadataKey
ACCOUNT_URN = URN("urn:aws:account:123456789012:us-east-1:root")


def _make_session(roles, users, policies):
    session = MagicMock()
    iam = MagicMock()
    session.client.return_value = iam

    # Roles paginator
    role_paginator = MagicMock()
    role_paginator.paginate.return_value = [{"Roles": roles}]

    # Users paginator
    user_paginator = MagicMock()
    user_paginator.paginate.return_value = [{"Users": users}]

    # Policies paginator
    policy_paginator = MagicMock()
    policy_paginator.paginate.return_value = [{"Policies": policies}]

    # Attached role policies paginator (empty by default)
    attached_paginator = MagicMock()
    attached_paginator.paginate.return_value = [{"AttachedPolicies": []}]

    # Attached user policies paginator (empty by default)
    attached_user_paginator = MagicMock()
    attached_user_paginator.paginate.return_value = [{"AttachedPolicies": []}]

    def get_paginator(name):
        if name == "list_roles":
            return role_paginator
        if name == "list_users":
            return user_paginator
        if name == "list_policies":
            return policy_paginator
        if name == "list_attached_role_policies":
            return attached_paginator
        if name == "list_attached_user_policies":
            return attached_user_paginator
        return MagicMock()

    iam.get_paginator = get_paginator
    iam.list_access_keys.return_value = {"AccessKeyMetadata": []}
    iam.list_mfa_devices.return_value = {"MFADevices": []}
    iam.get_policy_version.return_value = {
        "PolicyVersion": {"Document": {"Version": "2012-10-17", "Statement": []}},
    }

    return session


class TestIamResourcePlugin:
    def test_service_name(self):
        assert IamResourcePlugin().service_name() == "iam"

    def test_discover_roles(self):
        roles = [{
            "RoleName": "my-role",
            "Arn": "arn:aws:iam::123:role/my-role",
            "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": []},
        }]
        session = _make_session(roles, [], [])
        plugin = IamResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        role_nodes = [n for n in nodes if n.node_type == "iam_role"]
        assert len(role_nodes) == 1
        assert role_nodes[0].metadata[NK.ROLE_NAME] == "my-role"

    def test_discover_users(self):
        users = [{
            "UserName": "alice",
            "Arn": "arn:aws:iam::123:user/alice",
        }]
        session = _make_session([], users, [])
        plugin = IamResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        user_nodes = [n for n in nodes if n.node_type == "iam_user"]
        assert len(user_nodes) == 1
        assert user_nodes[0].metadata[NK.IAM_USER_NAME] == "alice"

    def test_discover_policies(self):
        policies = [{
            "PolicyName": "my-policy",
            "Arn": "arn:aws:iam::123:policy/my-policy",
            "DefaultVersionId": "v1",
        }]
        session = _make_session([], [], policies)
        plugin = IamResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        policy_nodes = [n for n in nodes if n.node_type == "iam_policy"]
        assert len(policy_nodes) == 1
        assert policy_nodes[0].metadata[NK.IAM_POLICY_NAME] == "my-policy"

    def test_discover_empty(self):
        session = _make_session([], [], [])
        plugin = IamResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 0
        assert len(edges) == 0
