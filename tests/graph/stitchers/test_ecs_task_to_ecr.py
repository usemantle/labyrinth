"""Tests for EcsTaskToEcrStitcher."""

import uuid

from src.graph.graph_models import URN, EdgeType, Graph, Node, NodeMetadata, NodeMetadataKey, NodeType
from src.graph.stitchers.ecs_task_to_ecr import EcsTaskToEcrStitcher

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
NK = NodeMetadataKey


def test_ecs_task_to_ecr_match():
    repo_uri = "123.dkr.ecr.us-east-1.amazonaws.com/my-app"
    ecr_node = Node(
        organization_id=ORG_ID,
        urn=URN("urn:aws:ecr:123:us-east-1:my-app"),
        node_type=NodeType.IMAGE_REPOSITORY,
        metadata=NodeMetadata({NK.REPOSITORY_URI: repo_uri}),
    )
    task_node = Node(
        organization_id=ORG_ID,
        urn=URN("urn:aws:ecs:123:us-east-1:task/my-task"),
        node_type=NodeType.ECS_TASK_DEFINITION,
        metadata=NodeMetadata({NK.ECS_CONTAINER_IMAGES: [f"{repo_uri}:latest"]}),
    )
    graph = Graph(nodes=[ecr_node, task_node])
    result = EcsTaskToEcrStitcher().stitch(ORG_ID, graph, {})

    refs = [e for e in result.edges if e.edge_type == EdgeType.REFERENCES]
    assert len(refs) == 1
    assert str(refs[0].from_urn) == str(task_node.urn)
    assert str(refs[0].to_urn) == str(ecr_node.urn)


def test_ecs_task_to_ecr_no_match():
    ecr_node = Node(
        organization_id=ORG_ID,
        urn=URN("urn:aws:ecr:123:us-east-1:my-app"),
        node_type=NodeType.IMAGE_REPOSITORY,
        metadata=NodeMetadata({NK.REPOSITORY_URI: "123.dkr.ecr.us-east-1.amazonaws.com/my-app"}),
    )
    task_node = Node(
        organization_id=ORG_ID,
        urn=URN("urn:aws:ecs:123:us-east-1:task/my-task"),
        node_type=NodeType.ECS_TASK_DEFINITION,
        metadata=NodeMetadata({NK.ECS_CONTAINER_IMAGES: ["other-registry.com/other:latest"]}),
    )
    graph = Graph(nodes=[ecr_node, task_node])
    result = EcsTaskToEcrStitcher().stitch(ORG_ID, graph, {})
    assert len(result.edges) == 0
