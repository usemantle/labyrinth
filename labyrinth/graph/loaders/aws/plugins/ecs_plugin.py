"""ECS resource discovery plugin for AwsAccountLoader."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable

import boto3

from labyrinth.graph.edges.assumes_edge import AssumesEdge
from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.protected_by_edge import ProtectedByEdge
from labyrinth.graph.edges.references_edge import ReferencesEdge
from labyrinth.graph.graph_models import URN, Edge, EdgeMetadata, EdgeMetadataKey, Node, NodeMetadataKey
from labyrinth.graph.loaders.aws.plugins._base import AwsResourcePlugin
from labyrinth.graph.nodes.ecs_cluster_node import EcsClusterNode
from labyrinth.graph.nodes.ecs_service_node import EcsServiceNode
from labyrinth.graph.nodes.ecs_task_definition_node import EcsTaskDefinitionNode
from labyrinth.graph.nodes.iam_role_node import IamRoleNode
from labyrinth.graph.nodes.security_group_node import SecurityGroupNode

logger = logging.getLogger(__name__)

EK = EdgeMetadataKey


class EcsResourcePlugin(AwsResourcePlugin):
    """Discover ECS clusters, services, and task definitions."""

    def service_name(self) -> str:
        return "ecs"

    def discover(
        self,
        session: boto3.Session,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        build_urn: Callable[..., URN],
    ) -> tuple[list[Node], list[Edge]]:
        ecs = session.client("ecs", region_name=region)
        nodes: list[Node] = []
        edges: list[Edge] = []

        try:
            cluster_arns = self._list_clusters(ecs)
        except Exception:
            logger.exception("Failed to list ECS clusters")
            return nodes, edges

        for cluster_arn in cluster_arns:
            cluster_name = cluster_arn.rsplit("/", 1)[-1]
            cluster_urn = EcsClusterNode.build_urn(account_id, region, cluster_name)

            cluster_node = EcsClusterNode.create(
                organization_id=organization_id,
                urn=cluster_urn,
                parent_urn=account_urn,
                cluster_name=cluster_name,
                arn=cluster_arn,
            )
            nodes.append(cluster_node)

            # Discover services in this cluster
            try:
                self._discover_services(
                    ecs, cluster_arn, cluster_name, cluster_urn,
                    account_id, region, organization_id, nodes, edges,
                )
            except Exception:
                logger.exception("Failed to discover services in cluster %s", cluster_name)

        return nodes, edges

    def _list_clusters(self, ecs) -> list[str]:
        arns: list[str] = []
        paginator = ecs.get_paginator("list_clusters")
        for page in paginator.paginate():
            arns.extend(page.get("clusterArns", []))
        return arns

    def _discover_services(
        self,
        ecs,
        cluster_arn: str,
        cluster_name: str,
        cluster_urn: URN,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        nodes: list[Node],
        edges: list[Edge],
    ) -> None:
        service_arns: list[str] = []
        paginator = ecs.get_paginator("list_services")
        for page in paginator.paginate(cluster=cluster_arn):
            service_arns.extend(page.get("serviceArns", []))

        if not service_arns:
            return

        # Describe services in batches of 10 (API limit)
        for i in range(0, len(service_arns), 10):
            batch = service_arns[i:i + 10]
            resp = ecs.describe_services(cluster=cluster_arn, services=batch)

            for svc in resp.get("services", []):
                svc_name = svc["serviceName"]
                svc_urn = EcsServiceNode.build_urn(
                    account_id, region, cluster_name, svc_name,
                )
                task_def_arn = svc.get("taskDefinition", "")

                svc_node = EcsServiceNode.create(
                    organization_id=organization_id,
                    urn=svc_urn,
                    parent_urn=cluster_urn,
                    service_name=svc_name,
                    task_definition=task_def_arn,
                    arn=svc.get("serviceArn"),
                )

                # Capture target group ARNs from load balancer config
                tg_arns = [
                    lb["targetGroupArn"]
                    for lb in svc.get("loadBalancers", [])
                    if "targetGroupArn" in lb
                ]
                if tg_arns:
                    svc_node.metadata[NodeMetadataKey.ECS_TARGET_GROUP_ARNS] = tg_arns

                nodes.append(svc_node)
                edges.append(ContainsEdge.create(organization_id, cluster_urn, svc_urn))

                # Security groups from network configuration
                net_config = svc.get("networkConfiguration", {}).get("awsvpcConfiguration", {})
                for sg_id in net_config.get("securityGroups", []):
                    sg_urn = SecurityGroupNode.build_urn(
                        account_id, region, "unknown", sg_id,
                    )
                    edges.append(ProtectedByEdge.create(organization_id, svc_urn, sg_urn))

                # Discover task definition
                if task_def_arn:
                    self._discover_task_definition(
                        ecs, task_def_arn, svc_urn,
                        account_id, region, organization_id, nodes, edges,
                    )

    def _discover_task_definition(
        self,
        ecs,
        task_def_arn: str,
        service_urn: URN,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        nodes: list[Node],
        edges: list[Edge],
    ) -> None:
        try:
            resp = ecs.describe_task_definition(taskDefinition=task_def_arn)
        except Exception:
            logger.debug("Failed to describe task definition %s", task_def_arn)
            return

        task_def = resp.get("taskDefinition", {})
        family = task_def.get("family", "unknown")
        revision = task_def.get("revision", 0)

        td_urn = EcsTaskDefinitionNode.build_urn(
            account_id, region, family, revision,
        )

        # Collect container images
        container_images = []
        for container in task_def.get("containerDefinitions", []):
            image = container.get("image")
            if image:
                container_images.append(image)

        task_role_arn = task_def.get("taskRoleArn")
        execution_role_arn = task_def.get("executionRoleArn")

        td_node = EcsTaskDefinitionNode.create(
            organization_id=organization_id,
            urn=td_urn,
            parent_urn=None,
            family=family,
            revision=revision,
            container_images=container_images or None,
            task_role_arn=task_role_arn,
            execution_role_arn=execution_role_arn,
            arn=task_def.get("taskDefinitionArn"),
        )
        nodes.append(td_node)

        # Service references task definition
        edges.append(ReferencesEdge.create(organization_id, service_urn, td_urn))

        # Task definition assumes IAM roles
        if task_role_arn:
            role_name = task_role_arn.rsplit("/", 1)[-1]
            role_urn = IamRoleNode.build_urn(account_id, role_name)
            edges.append(AssumesEdge.create(
                organization_id, td_urn, role_urn,
                metadata=EdgeMetadata({EK.ASSUMED_VIA: "taskRoleArn"}),
            ))

        if execution_role_arn:
            role_name = execution_role_arn.rsplit("/", 1)[-1]
            role_urn = IamRoleNode.build_urn(account_id, role_name)
            edges.append(AssumesEdge.create(
                organization_id, td_urn, role_urn,
                metadata=EdgeMetadata({EK.ASSUMED_VIA: "executionRoleArn"}),
            ))
