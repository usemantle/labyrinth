"""Tests for BuildsEdge."""

import uuid

from src.graph.edges.builds_edge import BuildsEdge
from src.graph.graph_models import URN, Edge, EdgeMetadata, EdgeMetadataKey

ORG_ID = uuid.uuid4()
FROM_URN = URN("urn:github:repo:org:::org/repo/Dockerfile")
TO_URN = URN("urn:aws:ecr:123:us-east-1:my-repo")


class TestBuildsEdge:
    def test_is_subclass_of_edge(self):
        assert issubclass(BuildsEdge, Edge)

    def test_edge_type(self):
        edge = BuildsEdge.create(ORG_ID, FROM_URN, TO_URN)
        assert edge.edge_type == "builds"

    def test_deterministic_uuid(self):
        e1 = BuildsEdge.create(ORG_ID, FROM_URN, TO_URN)
        e2 = BuildsEdge.create(ORG_ID, FROM_URN, TO_URN)
        assert e1.uuid == e2.uuid

    def test_different_urns_produce_different_uuid(self):
        e1 = BuildsEdge.create(ORG_ID, FROM_URN, TO_URN)
        e2 = BuildsEdge.create(ORG_ID, TO_URN, FROM_URN)
        assert e1.uuid != e2.uuid

    def test_custom_metadata(self):
        meta = EdgeMetadata({
            EdgeMetadataKey.DETECTION_METHOD: "oci_label",
            EdgeMetadataKey.CONFIDENCE: 1.0,
        })
        edge = BuildsEdge.create(ORG_ID, FROM_URN, TO_URN, metadata=meta)
        assert edge.metadata[EdgeMetadataKey.CONFIDENCE] == 1.0

    def test_fields(self):
        edge = BuildsEdge.create(ORG_ID, FROM_URN, TO_URN)
        assert edge.organization_id == ORG_ID
        assert edge.from_urn == FROM_URN
        assert edge.to_urn == TO_URN
