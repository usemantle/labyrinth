"""RDS resource discovery plugin for AwsAccountLoader."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable

import boto3

from labyrinth.graph.edges.protected_by_edge import ProtectedByEdge
from labyrinth.graph.graph_models import URN, Edge, Node
from labyrinth.graph.loaders.aws.plugins._base import AwsResourcePlugin
from labyrinth.graph.nodes.rds_cluster_node import RdsClusterNode

logger = logging.getLogger(__name__)


class RdsResourcePlugin(AwsResourcePlugin):
    """Discover RDS clusters and instances in the account."""

    def service_name(self) -> str:
        return "rds"

    def discover(
        self,
        session: boto3.Session,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        build_urn: Callable[..., URN],
    ) -> tuple[list[Node], list[Edge]]:
        rds = session.client("rds", region_name=region)
        nodes: list[Node] = []
        edges: list[Edge] = []

        # Discover RDS instances (covers both standalone and cluster members)
        try:
            paginator = rds.get_paginator("describe_db_instances")
            for page in paginator.paginate():
                for instance in page.get("DBInstances", []):
                    self._process_instance(
                        instance, account_id, region, organization_id,
                        account_urn, nodes, edges,
                    )
        except Exception:
            logger.exception("Failed to describe RDS instances")

        return nodes, edges

    def _process_instance(
        self,
        instance: dict,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        nodes: list[Node],
        edges: list[Edge],
    ) -> None:
        db_id = instance["DBInstanceIdentifier"]
        endpoint_info = instance.get("Endpoint", {})
        endpoint = endpoint_info.get("Address")
        port = endpoint_info.get("Port")

        rds_urn = URN(f"urn:aws:rds:{account_id}:{region}:{db_id}")

        node = RdsClusterNode.create(
            organization_id=organization_id,
            urn=rds_urn,
            parent_urn=account_urn,
            cluster_id=db_id,
            engine=instance.get("Engine"),
            endpoint=endpoint,
            port=port,
            publicly_accessible=instance.get("PubliclyAccessible"),
            encryption_enabled=instance.get("StorageEncrypted"),
            multi_az=instance.get("MultiAZ"),
            arn=instance.get("DBInstanceArn"),
        )
        nodes.append(node)

        # Create ProtectedByEdge for each security group
        for sg in instance.get("VpcSecurityGroups", []):
            sg_id = sg.get("VpcSecurityGroupId")
            if sg_id:
                vpc_id = instance.get("DBSubnetGroup", {}).get("VpcId", "unknown")
                sg_urn = URN(f"urn:aws:vpc:{account_id}:{region}:{vpc_id}/sg/{sg_id}")
                edges.append(ProtectedByEdge.create(
                    organization_id, rds_urn, sg_urn,
                ))
