"""Tests for typed edge subclasses."""

import uuid

import pytest

from src.graph.edges import (
    CallsEdge,
    ContainsEdge,
    DependsOnEdge,
    HostsEdge,
    InstantiatesEdge,
    ModelsEdge,
    ReadsEdge,
    ReferencesEdge,
    SoftReferenceEdge,
    WritesEdge,
)
from src.graph.graph_models import URN, Edge, EdgeMetadata, EdgeMetadataKey, RelationType

ORG_ID = uuid.uuid4()
FROM_URN = URN("urn:test:test:::from")
TO_URN = URN("urn:test:test:::to")


class TestEdgeSubclassIsEdge:
    """Every typed edge must be an instance of Edge."""

    @pytest.mark.parametrize("edge_cls", [
        ContainsEdge, HostsEdge, CallsEdge, InstantiatesEdge,
        ReadsEdge, WritesEdge, ModelsEdge,
        ReferencesEdge, SoftReferenceEdge, DependsOnEdge,
    ])
    def test_is_subclass_of_edge(self, edge_cls):
        assert issubclass(edge_cls, Edge)


class TestEdgeTypeAndRelationType:
    """Each typed edge must have correct edge_type and relation_type."""

    @pytest.mark.parametrize("edge_cls,expected_edge_type,expected_relation_type", [
        (ContainsEdge, "contains", RelationType.CONTAINS),
        (HostsEdge, "hosts", RelationType.HOSTS),
        (CallsEdge, "calls", RelationType.CODE_TO_CODE),
        (InstantiatesEdge, "instantiates", RelationType.CODE_TO_CODE),
        (ReadsEdge, "reads", RelationType.CODE_TO_DATA),
        (WritesEdge, "writes", RelationType.CODE_TO_DATA),
        (ModelsEdge, "models", RelationType.CODE_TO_DATA),
        (ReferencesEdge, "references", RelationType.DATA_TO_DATA),
        (SoftReferenceEdge, "soft_reference", RelationType.DATA_TO_DATA),
        (DependsOnEdge, "depends_on", RelationType.DEPENDS_ON),
    ])
    def test_edge_type_and_relation_type(self, edge_cls, expected_edge_type, expected_relation_type):
        edge = edge_cls.create(ORG_ID, FROM_URN, TO_URN)
        assert edge.edge_type == expected_edge_type
        assert edge.relation_type == expected_relation_type


class TestEdgeCreateDeterministicUUID:
    """Edge create() must produce deterministic UUIDs."""

    @pytest.mark.parametrize("edge_cls", [
        ContainsEdge, HostsEdge, CallsEdge, InstantiatesEdge,
        ReadsEdge, WritesEdge, ModelsEdge,
        ReferencesEdge, SoftReferenceEdge, DependsOnEdge,
    ])
    def test_deterministic_uuid(self, edge_cls):
        e1 = edge_cls.create(ORG_ID, FROM_URN, TO_URN)
        e2 = edge_cls.create(ORG_ID, FROM_URN, TO_URN)
        assert e1.uuid == e2.uuid

    @pytest.mark.parametrize("edge_cls", [
        ContainsEdge, HostsEdge, CallsEdge, InstantiatesEdge,
        ReadsEdge, WritesEdge, ModelsEdge,
        ReferencesEdge, SoftReferenceEdge, DependsOnEdge,
    ])
    def test_different_urns_produce_different_uuid(self, edge_cls):
        e1 = edge_cls.create(ORG_ID, FROM_URN, TO_URN)
        e2 = edge_cls.create(ORG_ID, TO_URN, FROM_URN)
        assert e1.uuid != e2.uuid


class TestEdgeCreateMetadata:
    """Edge create() must accept optional metadata."""

    def test_default_empty_metadata(self):
        edge = ContainsEdge.create(ORG_ID, FROM_URN, TO_URN)
        assert len(edge.metadata) == 0

    def test_custom_metadata(self):
        meta = EdgeMetadata({EdgeMetadataKey.CALL_TYPE: "direct"})
        edge = CallsEdge.create(ORG_ID, FROM_URN, TO_URN, metadata=meta)
        assert edge.metadata[EdgeMetadataKey.CALL_TYPE] == "direct"


class TestEdgeCreateFields:
    """Edge create() must set from_urn, to_urn, and organization_id."""

    def test_fields(self):
        edge = ReadsEdge.create(ORG_ID, FROM_URN, TO_URN)
        assert edge.organization_id == ORG_ID
        assert edge.from_urn == FROM_URN
        assert edge.to_urn == TO_URN


class TestDistinctEdgeTypesProduceDifferentUUIDs:
    """Different edge types between the same nodes must produce different UUIDs."""

    def test_reads_vs_writes(self):
        r = ReadsEdge.create(ORG_ID, FROM_URN, TO_URN)
        w = WritesEdge.create(ORG_ID, FROM_URN, TO_URN)
        assert r.uuid != w.uuid

    def test_models_vs_reads(self):
        m = ModelsEdge.create(ORG_ID, FROM_URN, TO_URN)
        r = ReadsEdge.create(ORG_ID, FROM_URN, TO_URN)
        assert m.uuid != r.uuid

    def test_references_vs_soft_reference(self):
        ref = ReferencesEdge.create(ORG_ID, FROM_URN, TO_URN)
        soft = SoftReferenceEdge.create(ORG_ID, FROM_URN, TO_URN)
        assert ref.uuid != soft.uuid

    def test_calls_vs_instantiates(self):
        c = CallsEdge.create(ORG_ID, FROM_URN, TO_URN)
        i = InstantiatesEdge.create(ORG_ID, FROM_URN, TO_URN)
        assert c.uuid != i.uuid


class TestEdgesWorkInListOfEdge:
    """Typed edges must be usable in list[Edge] contexts."""

    def test_mixed_list(self):
        edges: list[Edge] = [
            ContainsEdge.create(ORG_ID, FROM_URN, TO_URN),
            CallsEdge.create(ORG_ID, FROM_URN, TO_URN),
            ReadsEdge.create(ORG_ID, FROM_URN, TO_URN),
        ]
        assert len(edges) == 3
        assert all(isinstance(e, Edge) for e in edges)
