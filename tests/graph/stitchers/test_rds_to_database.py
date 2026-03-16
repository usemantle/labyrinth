"""Tests for RdsToDatabaseStitcher."""

import uuid

from src.graph.graph_models import URN, EdgeType, Graph, Node, NodeMetadata, NodeMetadataKey, NodeType
from src.graph.stitchers.rds_to_database import RdsToDatabaseStitcher

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey


def test_rds_to_database_endpoint_match():
    endpoint = "mydb.cluster-abc.us-east-1.rds.amazonaws.com"
    rds_node = Node(
        organization_id=ORG_ID,
        urn=URN("urn:aws:rds:123:us-east-1:mydb"),
        node_type=NodeType.RDS_CLUSTER,
        metadata=NodeMetadata({NK.RDS_ENDPOINT: endpoint}),
    )
    db_node = Node(
        organization_id=ORG_ID,
        urn=URN("urn:onprem:postgres:localhost:5432:mydb"),
        node_type=NodeType.DATABASE,
        metadata=NodeMetadata({NK.HOST: endpoint}),
    )
    graph = Graph(nodes=[rds_node, db_node])
    result = RdsToDatabaseStitcher().stitch(ORG_ID, graph, {})

    hosts = [e for e in result.edges if e.edge_type == EdgeType.HOSTS]
    assert len(hosts) == 1
    assert str(hosts[0].from_urn) == str(rds_node.urn)
    assert str(hosts[0].to_urn) == str(db_node.urn)


def test_rds_no_match():
    rds_node = Node(
        organization_id=ORG_ID,
        urn=URN("urn:aws:rds:123:us-east-1:mydb"),
        node_type=NodeType.RDS_CLUSTER,
        metadata=NodeMetadata({NK.RDS_ENDPOINT: "other.endpoint.com"}),
    )
    db_node = Node(
        organization_id=ORG_ID,
        urn=URN("urn:onprem:postgres:localhost:5432:mydb"),
        node_type=NodeType.DATABASE,
        metadata=NodeMetadata({NK.HOST: "different.host.com"}),
    )
    graph = Graph(nodes=[rds_node, db_node])
    result = RdsToDatabaseStitcher().stitch(ORG_ID, graph, {})
    assert len(result.edges) == 0
