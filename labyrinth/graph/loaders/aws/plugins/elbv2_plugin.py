"""ELBv2 resource discovery plugin for AwsAccountLoader."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable

import boto3

from labyrinth.graph.edges.protected_by_edge import ProtectedByEdge
from labyrinth.graph.edges.routes_to_edge import RoutesToEdge
from labyrinth.graph.graph_models import URN, Edge, EdgeMetadata, EdgeMetadataKey, Node
from labyrinth.graph.loaders.aws.plugins._base import AwsResourcePlugin
from labyrinth.graph.nodes.backend_group_node import BackendGroupNode
from labyrinth.graph.nodes.load_balancer_node import LoadBalancerNode
from labyrinth.graph.nodes.security_group_node import SecurityGroupNode

logger = logging.getLogger(__name__)

EK = EdgeMetadataKey


class Elbv2ResourcePlugin(AwsResourcePlugin):
    """Discover ALBs, NLBs, target groups, and their routing."""

    def service_name(self) -> str:
        return "elbv2"

    def discover(
        self,
        session: boto3.Session,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        build_urn: Callable[..., URN],
    ) -> tuple[list[Node], list[Edge]]:
        elbv2 = session.client("elbv2", region_name=region)
        nodes: list[Node] = []
        edges: list[Edge] = []

        try:
            load_balancers = self._describe_load_balancers(elbv2)
        except Exception:
            logger.exception("Failed to describe load balancers")
            return nodes, edges

        for lb in load_balancers:
            lb_name = lb["LoadBalancerName"]
            lb_type = lb.get("Type", "application").lower()
            lb_type_key = "alb" if lb_type == "application" else "nlb"
            lb_scheme = lb.get("Scheme", "internal")
            lb_dns_name = lb.get("DNSName", "")
            lb_arn = lb.get("LoadBalancerArn", "")
            lb_state = lb.get("State", {}).get("Code", "unknown")

            lb_urn = LoadBalancerNode.build_urn(account_id, region, lb_name)

            # Collect listeners
            listeners = self._describe_listeners(elbv2, lb_arn)
            listener_data = []
            for listener in listeners:
                listener_data.append({
                    "port": listener.get("Port"),
                    "protocol": listener.get("Protocol"),
                    "default_actions": [
                        a.get("Type") for a in listener.get("DefaultActions", [])
                    ],
                })

            lb_node = LoadBalancerNode.create(
                organization_id=organization_id,
                urn=lb_urn,
                parent_urn=account_urn,
                lb_type=lb_type_key,
                lb_scheme=lb_scheme,
                lb_dns_name=lb_dns_name,
                listeners=listener_data or None,
                lb_state=lb_state,
                arn=lb_arn,
            )
            nodes.append(lb_node)

            # Security groups
            for sg_id in lb.get("SecurityGroups", []):
                sg_urn = SecurityGroupNode.build_urn(account_id, region, sg_id)
                edges.append(ProtectedByEdge.create(organization_id, lb_urn, sg_urn))

            # Target groups for this LB
            self._discover_target_groups(
                elbv2, lb_arn, lb_name, lb_urn, listeners,
                account_id, region, organization_id, account_urn,
                nodes, edges,
            )

        logger.info("ELBv2: discovered %d nodes", len(nodes))
        return nodes, edges

    def _describe_load_balancers(self, elbv2) -> list[dict]:
        lbs: list[dict] = []
        paginator = elbv2.get_paginator("describe_load_balancers")
        for page in paginator.paginate():
            lbs.extend(page.get("LoadBalancers", []))
        return lbs

    def _describe_listeners(self, elbv2, lb_arn: str) -> list[dict]:
        try:
            paginator = elbv2.get_paginator("describe_listeners")
            listeners: list[dict] = []
            for page in paginator.paginate(LoadBalancerArn=lb_arn):
                listeners.extend(page.get("Listeners", []))
            return listeners
        except Exception:
            logger.debug("Failed to describe listeners for %s", lb_arn)
            return []

    def _discover_target_groups(
        self,
        elbv2,
        lb_arn: str,
        lb_name: str,
        lb_urn: URN,
        listeners: list[dict],
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        nodes: list[Node],
        edges: list[Edge],
    ) -> None:
        try:
            paginator = elbv2.get_paginator("describe_target_groups")
            target_groups: list[dict] = []
            for page in paginator.paginate(LoadBalancerArn=lb_arn):
                target_groups.extend(page.get("TargetGroups", []))
        except Exception:
            logger.debug("Failed to describe target groups for %s", lb_arn)
            return

        # Build listener port/protocol map for edge metadata
        listener_by_tg: dict[str, dict] = {}
        for listener in listeners:
            for action in listener.get("DefaultActions", []):
                tg_arn = action.get("TargetGroupArn", "")
                if tg_arn:
                    listener_by_tg[tg_arn] = {
                        "port": listener.get("Port"),
                        "protocol": listener.get("Protocol"),
                    }

        for tg in target_groups:
            tg_name = tg["TargetGroupName"]
            tg_arn = tg.get("TargetGroupArn", "")
            tg_urn = BackendGroupNode.build_urn(account_id, region, lb_name, tg_name)

            health_check = None
            if tg.get("HealthCheckEnabled"):
                health_check = {
                    "protocol": tg.get("HealthCheckProtocol"),
                    "path": tg.get("HealthCheckPath"),
                    "interval": tg.get("HealthCheckIntervalSeconds"),
                }

            bg_node = BackendGroupNode.create(
                organization_id=organization_id,
                urn=tg_urn,
                parent_urn=account_urn,
                bg_name=tg_name,
                bg_port=tg.get("Port"),
                bg_protocol=tg.get("Protocol"),
                bg_target_type=tg.get("TargetType"),
                bg_health_check=health_check,
                bg_backend_type="aws_target_group",
                arn=tg_arn,
            )
            nodes.append(bg_node)

            # LB -> target group edge
            edge_meta = EdgeMetadata()
            listener_info = listener_by_tg.get(tg_arn, {})
            if listener_info.get("port"):
                edge_meta[EK.LISTENER_PORT] = listener_info["port"]
            if listener_info.get("protocol"):
                edge_meta[EK.LISTENER_PROTOCOL] = listener_info["protocol"]
            edges.append(RoutesToEdge.create(
                organization_id, lb_urn, tg_urn, metadata=edge_meta,
            ))
