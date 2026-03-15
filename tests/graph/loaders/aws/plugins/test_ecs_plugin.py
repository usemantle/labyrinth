"""Tests for EcsResourcePlugin."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, call

from src.graph.graph_models import URN, NodeMetadataKey
from src.graph.loaders.aws.plugins.ecs_plugin import EcsResourcePlugin

ORG_ID = uuid.uuid4()
NK = NodeMetadataKey
ACCOUNT_URN = URN("urn:aws:account:123456789012:us-east-1:root")


def _make_session(cluster_arns, services, task_def):
    session = MagicMock()
    ecs = MagicMock()
    session.client.return_value = ecs

    # list_clusters paginator
    cluster_paginator = MagicMock()
    cluster_paginator.paginate.return_value = [{"clusterArns": cluster_arns}]

    # list_services paginator
    service_paginator = MagicMock()
    service_paginator.paginate.return_value = [{"serviceArns": [s["serviceArn"] for s in services]}]

    def get_paginator(name):
        if name == "list_clusters":
            return cluster_paginator
        if name == "list_services":
            return service_paginator
        return MagicMock()

    ecs.get_paginator = get_paginator
    ecs.describe_services.return_value = {"services": services}
    ecs.describe_task_definition.return_value = {"taskDefinition": task_def}

    return session


class TestEcsResourcePlugin:
    def test_service_name(self):
        assert EcsResourcePlugin().service_name() == "ecs"

    def test_discover_cluster_service_taskdef(self):
        cluster_arns = ["arn:aws:ecs:us-east-1:123:cluster/prod"]
        services = [{
            "serviceName": "api",
            "serviceArn": "arn:aws:ecs:us-east-1:123:service/prod/api",
            "taskDefinition": "arn:aws:ecs:us-east-1:123:task-definition/api:5",
            "networkConfiguration": {
                "awsvpcConfiguration": {"securityGroups": ["sg-svc"]},
            },
        }]
        task_def = {
            "family": "api",
            "revision": 5,
            "taskDefinitionArn": "arn:aws:ecs:us-east-1:123:task-definition/api:5",
            "taskRoleArn": "arn:aws:iam::123:role/api-task-role",
            "executionRoleArn": "arn:aws:iam::123:role/ecs-exec-role",
            "containerDefinitions": [
                {"name": "app", "image": "123.dkr.ecr.us-east-1.amazonaws.com/api:latest"},
            ],
        }

        session = _make_session(cluster_arns, services, task_def)
        plugin = EcsResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        # Cluster + Service + TaskDef
        node_types = {n.node_type for n in nodes}
        assert "ecs_cluster" in node_types
        assert "ecs_service" in node_types
        assert "ecs_task_definition" in node_types

        # Check edges
        edge_types = {e.edge_type for e in edges}
        assert "contains" in edge_types  # cluster -> service
        assert "references" in edge_types  # service -> taskdef
        assert "assumes" in edge_types  # taskdef -> IAM role
        assert "protected_by" in edge_types  # service -> SG

    def test_discover_no_clusters(self):
        session = _make_session([], [], {})
        plugin = EcsResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 0
        assert len(edges) == 0
