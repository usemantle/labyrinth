"""Tests for AWS node types."""

from __future__ import annotations

import uuid

import pytest

from labyrinth.graph.graph_models import URN, NodeMetadataKey
from labyrinth.graph.nodes.aws_account_node import AwsAccountNode
from labyrinth.graph.nodes.ecs_cluster_node import EcsClusterNode
from labyrinth.graph.nodes.ecs_service_node import EcsServiceNode
from labyrinth.graph.nodes.ecs_task_definition_node import EcsTaskDefinitionNode
from labyrinth.graph.nodes.iam_policy_node import IamPolicyNode
from labyrinth.graph.nodes.iam_role_node import IamRoleNode
from labyrinth.graph.nodes.iam_user_node import IamUserNode
from labyrinth.graph.nodes.nacl_node import NaclNode
from labyrinth.graph.nodes.rds_cluster_node import RdsClusterNode
from labyrinth.graph.nodes.security_group_node import SecurityGroupNode
from labyrinth.graph.nodes.sso_group_node import SsoGroupNode
from labyrinth.graph.nodes.vpc_node import VpcNode

NK = NodeMetadataKey
ORG_ID = uuid.uuid4()


class TestAwsAccountNode:
    def test_create(self):
        urn = URN("urn:aws:account:123456789012:us-east-1:root")
        node = AwsAccountNode.create(
            organization_id=ORG_ID, urn=urn,
            account_id="123456789012", region="us-east-1",
        )
        assert node.node_type == "aws_account"
        assert node.metadata[NK.ACCOUNT_ID] == "123456789012"
        assert node.metadata[NK.REGION] == "us-east-1"


class TestRdsClusterNode:
    def test_create_minimal(self):
        urn = URN("urn:aws:rds:123456789012:us-east-1:my-db")
        node = RdsClusterNode.create(
            organization_id=ORG_ID, urn=urn, cluster_id="my-db",
        )
        assert node.node_type == "rds_cluster"
        assert node.metadata[NK.RDS_CLUSTER_ID] == "my-db"

    def test_create_full(self):
        urn = URN("urn:aws:rds:123456789012:us-east-1:my-db")
        node = RdsClusterNode.create(
            organization_id=ORG_ID, urn=urn,
            cluster_id="my-db", engine="postgres",
            endpoint="my-db.abc.us-east-1.rds.amazonaws.com",
            port=5432, publicly_accessible=True,
            encryption_enabled=False, multi_az=True,
        )
        assert node.metadata[NK.RDS_ENGINE] == "postgres"
        assert node.metadata[NK.RDS_PUBLICLY_ACCESSIBLE] is True
        assert node.metadata[NK.RDS_ENCRYPTION_ENABLED] is False


class TestEcsClusterNode:
    def test_create(self):
        urn = URN("urn:aws:ecs:123456789012:us-east-1:my-cluster")
        node = EcsClusterNode.create(
            organization_id=ORG_ID, urn=urn, cluster_name="my-cluster",
        )
        assert node.node_type == "ecs_cluster"
        assert node.metadata[NK.ECS_CLUSTER_NAME] == "my-cluster"


class TestEcsServiceNode:
    def test_create(self):
        urn = URN("urn:aws:ecs:123456789012:us-east-1:my-cluster/my-service")
        node = EcsServiceNode.create(
            organization_id=ORG_ID, urn=urn,
            service_name="my-service", task_definition="arn:aws:ecs:...",
        )
        assert node.node_type == "ecs_service"
        assert node.metadata[NK.ECS_SERVICE_NAME] == "my-service"


class TestEcsTaskDefinitionNode:
    def test_create(self):
        urn = URN("urn:aws:ecs:123456789012:us-east-1:taskdef/my-task:1")
        node = EcsTaskDefinitionNode.create(
            organization_id=ORG_ID, urn=urn,
            family="my-task", revision=1,
            container_images=["123456789012.dkr.ecr.us-east-1.amazonaws.com/my-app:latest"],
        )
        assert node.node_type == "ecs_task_definition"
        assert node.metadata[NK.ECS_TASK_FAMILY] == "my-task"
        assert len(node.metadata[NK.ECS_CONTAINER_IMAGES]) == 1


class TestVpcNode:
    def test_create(self):
        urn = URN("urn:aws:vpc:123456789012:us-east-1:vpc-123")
        node = VpcNode.create(
            organization_id=ORG_ID, urn=urn,
            vpc_id="vpc-123", cidr="10.0.0.0/16",
        )
        assert node.node_type == "vpc"
        assert node.metadata[NK.VPC_ID] == "vpc-123"
        assert node.metadata[NK.VPC_CIDR] == "10.0.0.0/16"


class TestSecurityGroupNode:
    def test_create(self):
        urn = URN("urn:aws:vpc:123456789012:us-east-1:vpc-123/sg/sg-abc")
        node = SecurityGroupNode.create(
            organization_id=ORG_ID, urn=urn,
            sg_id="sg-abc", sg_name="my-sg",
            rules_ingress=[{"protocol": "tcp", "from_port": 443, "to_port": 443}],
        )
        assert node.node_type == "security_group"
        assert node.metadata[NK.SG_ID] == "sg-abc"
        assert len(node.metadata[NK.SG_RULES_INGRESS]) == 1


class TestNaclNode:
    def test_create(self):
        urn = URN("urn:aws:vpc:123456789012:us-east-1:vpc-123/nacl/acl-xyz")
        node = NaclNode.create(
            organization_id=ORG_ID, urn=urn,
            nacl_id="acl-xyz", rules=[{"rule_number": 100}],
        )
        assert node.node_type == "nacl"
        assert node.metadata[NK.NACL_ID] == "acl-xyz"


class TestIamRoleNode:
    def test_create(self):
        urn = URN("urn:aws:iam:123456789012::role/my-role")
        node = IamRoleNode.create(
            organization_id=ORG_ID, urn=urn,
            role_name="my-role",
            trust_policy={"Version": "2012-10-17", "Statement": []},
        )
        assert node.node_type == "iam_role"
        assert node.metadata[NK.ROLE_NAME] == "my-role"
        assert node.metadata[NK.IAM_TRUST_POLICY]["Version"] == "2012-10-17"


class TestIamUserNode:
    def test_create(self):
        urn = URN("urn:aws:iam:123456789012::user/alice")
        node = IamUserNode.create(
            organization_id=ORG_ID, urn=urn,
            user_name="alice", mfa_enabled=True,
            access_keys=[{"id": "AKIA...", "status": "Active"}],
        )
        assert node.node_type == "iam_user"
        assert node.metadata[NK.IAM_USER_NAME] == "alice"
        assert node.metadata[NK.IAM_MFA_ENABLED] is True


class TestIamPolicyNode:
    def test_create(self):
        urn = URN("urn:aws:iam:123456789012::policy/my-policy")
        doc = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]}
        node = IamPolicyNode.create(
            organization_id=ORG_ID, urn=urn,
            policy_name="my-policy", policy_document=doc,
        )
        assert node.node_type == "iam_policy"
        assert node.metadata[NK.IAM_POLICY_NAME] == "my-policy"


class TestSsoGroupNode:
    def test_create(self):
        urn = URN("urn:aws:sso:123456789012::group/g-abc123")
        node = SsoGroupNode.create(
            organization_id=ORG_ID, urn=urn,
            group_id="g-abc123", group_name="Developers",
        )
        assert node.node_type == "sso_group"
        assert node.metadata[NK.SSO_GROUP_ID] == "g-abc123"
        assert node.metadata[NK.SSO_GROUP_NAME] == "Developers"
