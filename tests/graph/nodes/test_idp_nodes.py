"""Tests for IdP node types: PersonNode, GroupNode, ApplicationNode, SsoUserNode."""

from __future__ import annotations

import uuid

import pytest

from src.graph.edges.member_of_edge import MemberOfEdge
from src.graph.edges.okta_edges import (
    OktaAssignedToEdge,
    OktaMapsToEdge,
    OktaPartOfEdge,
    OktaPushesToEdge,
)
from src.graph.graph_models import URN, Node, NodeMetadataKey
from src.graph.nodes.aws.sso_user_node import SsoUserNode
from src.graph.nodes.okta.application_node import ApplicationNode
from src.graph.nodes.okta.group_node import GroupNode
from src.graph.nodes.okta.person_node import PersonNode

NK = NodeMetadataKey
ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


class TestPersonNode:
    def test_node_type(self):
        node = PersonNode.create(
            ORG_ID, URN("urn:okta:idp:yourorg.okta.com::user/00u1"),
            okta_id="00u1",
        )
        assert isinstance(node, Node)
        assert node.node_type == "person"

    def test_metadata(self):
        node = PersonNode.create(
            ORG_ID, URN("urn:okta:idp:yourorg.okta.com::user/00u1"),
            okta_id="00u1",
            email="alice@example.com",
            login="alice@example.com",
            status="ACTIVE",
            display_name="Alice Anderson",
        )
        assert node.metadata[NK.PERSON_OKTA_ID] == "00u1"
        assert node.metadata[NK.PERSON_EMAIL] == "alice@example.com"
        assert node.metadata[NK.PERSON_LOGIN] == "alice@example.com"
        assert node.metadata[NK.PERSON_STATUS] == "ACTIVE"
        assert node.metadata[NK.PERSON_DISPLAY_NAME] == "Alice Anderson"

    def test_optional_fields_omitted(self):
        node = PersonNode.create(
            ORG_ID, URN("urn:okta:idp:yourorg.okta.com::user/00u1"),
            okta_id="00u1",
        )
        assert NK.PERSON_EMAIL not in node.metadata
        assert NK.PERSON_LOGIN not in node.metadata

    @pytest.mark.parametrize("edge_cls", [
        OktaPartOfEdge, OktaAssignedToEdge, OktaMapsToEdge,
    ])
    def test_allowed_outgoing_edges(self, edge_cls):
        assert edge_cls in PersonNode._allowed_outgoing_edges


class TestGroupNode:
    def test_node_type(self):
        node = GroupNode.create(
            ORG_ID, URN("urn:okta:idp:yourorg.okta.com::group/00g1"),
            okta_id="00g1",
        )
        assert node.node_type == "group"

    def test_metadata(self):
        node = GroupNode.create(
            ORG_ID, URN("urn:okta:idp:yourorg.okta.com::group/00g1"),
            okta_id="00g1",
            name="Engineering",
            description="All engineering staff",
        )
        assert node.metadata[NK.GROUP_OKTA_ID] == "00g1"
        assert node.metadata[NK.GROUP_NAME] == "Engineering"
        assert node.metadata[NK.GROUP_DESCRIPTION] == "All engineering staff"

    def test_allowed_edges(self):
        assert OktaAssignedToEdge in GroupNode._allowed_outgoing_edges
        assert OktaPushesToEdge in GroupNode._allowed_outgoing_edges
        assert OktaPartOfEdge in GroupNode._allowed_incoming_edges


class TestApplicationNode:
    def test_node_type(self):
        node = ApplicationNode.create(
            ORG_ID, URN("urn:okta:idp:yourorg.okta.com::app/0oa1"),
            okta_id="0oa1",
        )
        assert node.node_type == "application"

    def test_metadata(self):
        node = ApplicationNode.create(
            ORG_ID, URN("urn:okta:idp:yourorg.okta.com::app/0oa1"),
            okta_id="0oa1",
            name="amazon_aws",
            label="AWS Account Federation",
            sign_on_mode="SAML_2_0",
            status="ACTIVE",
        )
        assert node.metadata[NK.APP_OKTA_ID] == "0oa1"
        assert node.metadata[NK.APP_NAME] == "amazon_aws"
        assert node.metadata[NK.APP_LABEL] == "AWS Account Federation"
        assert node.metadata[NK.APP_SIGN_ON_MODE] == "SAML_2_0"
        assert node.metadata[NK.APP_STATUS] == "ACTIVE"

    def test_allowed_incoming_edges(self):
        assert OktaAssignedToEdge in ApplicationNode._allowed_incoming_edges
        assert OktaPushesToEdge in ApplicationNode._allowed_incoming_edges


class TestSsoUserNode:
    def test_node_type(self):
        node = SsoUserNode.create(
            ORG_ID, URN("urn:aws:sso:123::user/u-abc"),
            user_id="u-abc",
        )
        assert node.node_type == "sso_user"

    def test_metadata(self):
        node = SsoUserNode.create(
            ORG_ID, URN("urn:aws:sso:123::user/u-abc"),
            user_id="u-abc",
            user_name="alice",
            email="alice@example.com",
            external_id="00u1",
        )
        assert node.metadata[NK.SSO_USER_ID] == "u-abc"
        assert node.metadata[NK.SSO_USER_NAME] == "alice"
        assert node.metadata[NK.SSO_USER_EMAIL] == "alice@example.com"
        assert node.metadata[NK.SSO_USER_EXTERNAL_ID] == "00u1"

    def test_allowed_edges(self):
        assert MemberOfEdge in SsoUserNode._allowed_outgoing_edges
        assert OktaMapsToEdge in SsoUserNode._allowed_incoming_edges
