"""Tests for OktaToIdentityCenterStitcher."""

from __future__ import annotations

import uuid

from src.graph.graph_models import URN, EdgeMetadataKey, EdgeType, Graph
from src.graph.nodes.person_node import PersonNode
from src.graph.nodes.sso_user_node import SsoUserNode
from src.graph.stitchers.okta_to_identity_center import OktaToIdentityCenterStitcher

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
EK = EdgeMetadataKey


def _person(okta_id: str, email: str | None = None) -> PersonNode:
    return PersonNode.create(
        ORG_ID, URN(f"urn:okta:idp:yourorg.okta.com::user/{okta_id}"),
        okta_id=okta_id, email=email,
    )


def _sso_user(user_id: str, *, email: str | None = None,
              external_id: str | None = None) -> SsoUserNode:
    return SsoUserNode.create(
        ORG_ID, URN(f"urn:aws:sso:123::user/{user_id}"),
        user_id=user_id, email=email, external_id=external_id,
    )


class TestOktaToIdentityCenter:
    def test_match_by_external_id(self):
        person = _person("00u1", email="alice@example.com")
        sso = _sso_user("u-abc", email="different@example.com", external_id="00u1")
        graph = Graph(nodes=[person, sso])

        result = OktaToIdentityCenterStitcher().stitch(ORG_ID, graph, {})

        edges = [e for e in result.edges if e.edge_type == EdgeType.IDP_MAPS_TO]
        assert len(edges) == 1
        edge = edges[0]
        assert str(edge.from_urn) == str(person.urn)
        assert str(edge.to_urn) == str(sso.urn)
        assert edge.metadata[EK.MATCH_KEY] == "externalId"
        assert edge.metadata[EK.MATCH_VALUE] == "00u1"
        assert edge.metadata[EK.CONFIDENCE] == 1.0

    def test_match_by_email_when_no_external_id(self):
        person = _person("00u1", email="Alice@Example.com")
        sso = _sso_user("u-abc", email="alice@example.com", external_id=None)
        graph = Graph(nodes=[person, sso])

        result = OktaToIdentityCenterStitcher().stitch(ORG_ID, graph, {})

        edges = [e for e in result.edges if e.edge_type == EdgeType.IDP_MAPS_TO]
        assert len(edges) == 1
        edge = edges[0]
        assert edge.metadata[EK.MATCH_KEY] == "email"
        assert edge.metadata[EK.MATCH_VALUE] == "alice@example.com"
        assert edge.metadata[EK.CONFIDENCE] == 0.85

    def test_external_id_preferred_over_email(self):
        # Person with both email and a matching externalId should match the externalId target.
        person = _person("00u-bob", email="ambiguous@example.com")
        sso_by_email = _sso_user("u-by-email", email="ambiguous@example.com", external_id=None)
        sso_by_eid = _sso_user("u-by-eid", email="other@example.com", external_id="00u-bob")
        graph = Graph(nodes=[person, sso_by_email, sso_by_eid])

        result = OktaToIdentityCenterStitcher().stitch(ORG_ID, graph, {})

        edges = [e for e in result.edges if e.edge_type == EdgeType.IDP_MAPS_TO]
        assert len(edges) == 1
        assert str(edges[0].to_urn) == str(sso_by_eid.urn)
        assert edges[0].metadata[EK.MATCH_KEY] == "externalId"

    def test_no_match_returns_no_edges(self):
        person = _person("00u1", email="alice@example.com")
        sso = _sso_user("u-abc", email="someoneelse@example.com", external_id="not-matching")
        graph = Graph(nodes=[person, sso])

        result = OktaToIdentityCenterStitcher().stitch(ORG_ID, graph, {})
        assert len(result.edges) == 0

    def test_empty_graph(self):
        result = OktaToIdentityCenterStitcher().stitch(ORG_ID, Graph(), {})
        assert len(result.edges) == 0

    def test_only_persons_no_sso_users(self):
        graph = Graph(nodes=[_person("00u1", email="a@x.com")])
        result = OktaToIdentityCenterStitcher().stitch(ORG_ID, graph, {})
        assert len(result.edges) == 0
