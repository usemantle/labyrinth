"""Tests for IdP-namespaced edges (idp:assigned_to, idp:maps_to, idp:part_of, idp:pushes_to)."""

from __future__ import annotations

import uuid

import pytest

from src.graph.edges.idp_assigned_to_edge import IdpAssignedToEdge
from src.graph.edges.idp_maps_to_edge import IdpMapsToEdge
from src.graph.edges.idp_part_of_edge import IdpPartOfEdge
from src.graph.edges.idp_pushes_to_edge import IdpPushesToEdge
from src.graph.graph_models import URN, Edge, EdgeMetadata, EdgeMetadataKey

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
FROM_URN = URN("urn:test:test:::from")
TO_URN = URN("urn:test:test:::to")

ALL_IDP_EDGES = [IdpAssignedToEdge, IdpMapsToEdge, IdpPartOfEdge, IdpPushesToEdge]


class TestIdpEdgesAreEdge:
    @pytest.mark.parametrize("edge_cls", ALL_IDP_EDGES)
    def test_is_subclass_of_edge(self, edge_cls):
        assert issubclass(edge_cls, Edge)


class TestIdpEdgeType:
    @pytest.mark.parametrize("edge_cls,expected", [
        (IdpAssignedToEdge, "idp:assigned_to"),
        (IdpMapsToEdge, "idp:maps_to"),
        (IdpPartOfEdge, "idp:part_of"),
        (IdpPushesToEdge, "idp:pushes_to"),
    ])
    def test_edge_type_carries_idp_namespace(self, edge_cls, expected):
        edge = edge_cls.create(ORG_ID, FROM_URN, TO_URN)
        assert edge.edge_type == expected
        assert edge.edge_type.startswith("idp:")


class TestDeterministicUUID:
    @pytest.mark.parametrize("edge_cls", ALL_IDP_EDGES)
    def test_same_inputs_produce_same_uuid(self, edge_cls):
        e1 = edge_cls.create(ORG_ID, FROM_URN, TO_URN)
        e2 = edge_cls.create(ORG_ID, FROM_URN, TO_URN)
        assert e1.uuid == e2.uuid

    @pytest.mark.parametrize("edge_cls", ALL_IDP_EDGES)
    def test_reversed_urns_produce_different_uuid(self, edge_cls):
        e1 = edge_cls.create(ORG_ID, FROM_URN, TO_URN)
        e2 = edge_cls.create(ORG_ID, TO_URN, FROM_URN)
        assert e1.uuid != e2.uuid

    def test_distinct_idp_edge_types_produce_different_uuids(self):
        # Same endpoints but different edge types must not collide.
        a = IdpAssignedToEdge.create(ORG_ID, FROM_URN, TO_URN)
        m = IdpMapsToEdge.create(ORG_ID, FROM_URN, TO_URN)
        p = IdpPartOfEdge.create(ORG_ID, FROM_URN, TO_URN)
        u = IdpPushesToEdge.create(ORG_ID, FROM_URN, TO_URN)
        assert len({a.uuid, m.uuid, p.uuid, u.uuid}) == 4


class TestMetadata:
    def test_default_metadata_empty(self):
        edge = IdpAssignedToEdge.create(ORG_ID, FROM_URN, TO_URN)
        assert len(edge.metadata) == 0

    def test_match_key_metadata_round_trip(self):
        meta = EdgeMetadata({
            EdgeMetadataKey.MATCH_KEY: "externalId",
            EdgeMetadataKey.MATCH_VALUE: "00uxyz",
            EdgeMetadataKey.CONFIDENCE: 1.0,
        })
        edge = IdpMapsToEdge.create(ORG_ID, FROM_URN, TO_URN, metadata=meta)
        assert edge.metadata[EdgeMetadataKey.MATCH_KEY] == "externalId"
        assert edge.metadata[EdgeMetadataKey.MATCH_VALUE] == "00uxyz"
        assert edge.metadata[EdgeMetadataKey.CONFIDENCE] == 1.0
