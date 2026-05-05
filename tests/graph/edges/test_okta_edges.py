"""Tests for Okta-namespaced edges (okta:assigned_to, okta:maps_to, okta:part_of, okta:pushes_to)."""

from __future__ import annotations

import uuid

import pytest

from labyrinth.graph.edges.okta_edges import (
    OktaAssignedToEdge,
    OktaMapsToEdge,
    OktaPartOfEdge,
    OktaPushesToEdge,
)
from labyrinth.graph.graph_models import URN, Edge, EdgeMetadata, EdgeMetadataKey

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
FROM_URN = URN("urn:test:test:::from")
TO_URN = URN("urn:test:test:::to")

ALL_OKTA_EDGES = [OktaAssignedToEdge, OktaMapsToEdge, OktaPartOfEdge, OktaPushesToEdge]


class TestOktaEdgesAreEdge:
    @pytest.mark.parametrize("edge_cls", ALL_OKTA_EDGES)
    def test_is_subclass_of_edge(self, edge_cls):
        assert issubclass(edge_cls, Edge)


class TestOktaEdgeType:
    @pytest.mark.parametrize("edge_cls,expected", [
        (OktaAssignedToEdge, "okta:assigned_to"),
        (OktaMapsToEdge, "okta:maps_to"),
        (OktaPartOfEdge, "okta:part_of"),
        (OktaPushesToEdge, "okta:pushes_to"),
    ])
    def test_edge_type_carries_okta_namespace(self, edge_cls, expected):
        edge = edge_cls.create(ORG_ID, FROM_URN, TO_URN)
        assert edge.edge_type == expected
        assert edge.edge_type.startswith("okta:")


class TestDeterministicUUID:
    @pytest.mark.parametrize("edge_cls", ALL_OKTA_EDGES)
    def test_same_inputs_produce_same_uuid(self, edge_cls):
        e1 = edge_cls.create(ORG_ID, FROM_URN, TO_URN)
        e2 = edge_cls.create(ORG_ID, FROM_URN, TO_URN)
        assert e1.uuid == e2.uuid

    @pytest.mark.parametrize("edge_cls", ALL_OKTA_EDGES)
    def test_reversed_urns_produce_different_uuid(self, edge_cls):
        e1 = edge_cls.create(ORG_ID, FROM_URN, TO_URN)
        e2 = edge_cls.create(ORG_ID, TO_URN, FROM_URN)
        assert e1.uuid != e2.uuid

    def test_distinct_okta_edge_types_produce_different_uuids(self):
        # Same endpoints but different edge types must not collide.
        a = OktaAssignedToEdge.create(ORG_ID, FROM_URN, TO_URN)
        m = OktaMapsToEdge.create(ORG_ID, FROM_URN, TO_URN)
        p = OktaPartOfEdge.create(ORG_ID, FROM_URN, TO_URN)
        u = OktaPushesToEdge.create(ORG_ID, FROM_URN, TO_URN)
        assert len({a.uuid, m.uuid, p.uuid, u.uuid}) == 4


class TestMetadata:
    def test_default_metadata_empty(self):
        edge = OktaAssignedToEdge.create(ORG_ID, FROM_URN, TO_URN)
        assert len(edge.metadata) == 0

    def test_match_key_metadata_round_trip(self):
        meta = EdgeMetadata({
            EdgeMetadataKey.MATCH_KEY: "externalId",
            EdgeMetadataKey.MATCH_VALUE: "00uxyz",
            EdgeMetadataKey.CONFIDENCE: 1.0,
        })
        edge = OktaMapsToEdge.create(ORG_ID, FROM_URN, TO_URN, metadata=meta)
        assert edge.metadata[EdgeMetadataKey.MATCH_KEY] == "externalId"
        assert edge.metadata[EdgeMetadataKey.MATCH_VALUE] == "00uxyz"
        assert edge.metadata[EdgeMetadataKey.CONFIDENCE] == 1.0
