"""VPC resource discovery plugin for AwsAccountLoader."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable

import boto3

from labyrinth.graph.edges.allows_traffic_to_edge import AllowsTrafficToEdge
from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.graph_models import URN, Edge, EdgeMetadata, EdgeMetadataKey, Node
from labyrinth.graph.loaders.aws.plugins._base import AwsResourcePlugin
from labyrinth.graph.nodes.nacl_node import NaclNode
from labyrinth.graph.nodes.security_group_node import SecurityGroupNode
from labyrinth.graph.nodes.vpc_node import VpcNode

logger = logging.getLogger(__name__)

EK = EdgeMetadataKey


class VpcResourcePlugin(AwsResourcePlugin):
    """Discover VPCs, security groups, and NACLs."""

    def service_name(self) -> str:
        return "vpc"

    def discover(
        self,
        session: boto3.Session,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        build_urn: Callable[..., URN],
    ) -> tuple[list[Node], list[Edge]]:
        ec2 = session.client("ec2", region_name=region)
        nodes: list[Node] = []
        edges: list[Edge] = []

        # Discover VPCs
        try:
            vpcs = ec2.describe_vpcs().get("Vpcs", [])
        except Exception:
            logger.exception("Failed to describe VPCs")
            return nodes, edges

        for vpc in vpcs:
            vpc_id = vpc["VpcId"]
            vpc_urn = VpcNode.build_urn(account_id, region, vpc_id)

            vpc_node = VpcNode.create(
                organization_id=organization_id,
                urn=vpc_urn,
                parent_urn=account_urn,
                vpc_id=vpc_id,
                cidr=vpc.get("CidrBlock"),
            )
            nodes.append(vpc_node)

        # Discover security groups
        try:
            self._discover_security_groups(
                ec2, account_id, region, organization_id, account_urn, nodes, edges,
            )
        except Exception:
            logger.exception("Failed to describe security groups")

        # Discover NACLs
        try:
            self._discover_nacls(
                ec2, account_id, region, organization_id, nodes, edges,
            )
        except Exception:
            logger.exception("Failed to describe NACLs")

        return nodes, edges

    def _discover_security_groups(
        self,
        ec2,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        nodes: list[Node],
        edges: list[Edge],
    ) -> None:
        paginator = ec2.get_paginator("describe_security_groups")
        for page in paginator.paginate():
            for sg in page.get("SecurityGroups", []):
                sg_id = sg["GroupId"]
                vpc_id = sg.get("VpcId", "unknown")
                sg_urn = SecurityGroupNode.build_urn(account_id, region, vpc_id, sg_id)
                vpc_urn = VpcNode.build_urn(account_id, region, vpc_id)

                # Parse rules into structured format
                ingress_rules = self._parse_rules(sg.get("IpPermissions", []))
                egress_rules = self._parse_rules(sg.get("IpPermissionsEgress", []))

                sg_node = SecurityGroupNode.create(
                    organization_id=organization_id,
                    urn=sg_urn,
                    parent_urn=vpc_urn,
                    sg_id=sg_id,
                    sg_name=sg.get("GroupName"),
                    rules_ingress=ingress_rules,
                    rules_egress=egress_rules,
                    vpc_id=vpc_id,
                )
                nodes.append(sg_node)
                edges.append(ContainsEdge.create(organization_id, vpc_urn, sg_urn))

                # Create AllowsTrafficToEdge for SG-to-SG references in ingress rules
                for rule in sg.get("IpPermissions", []):
                    for sg_ref in rule.get("UserIdGroupPairs", []):
                        ref_sg_id = sg_ref.get("GroupId")
                        if ref_sg_id:
                            ref_vpc_id = sg_ref.get("VpcId", vpc_id)
                            ref_sg_urn = SecurityGroupNode.build_urn(
                                account_id, region, ref_vpc_id, ref_sg_id,
                            )
                            port_range = self._format_port_range(rule)
                            edges.append(AllowsTrafficToEdge.create(
                                organization_id,
                                ref_sg_urn,
                                sg_urn,
                                metadata=EdgeMetadata({
                                    EK.SG_RULE_PROTOCOL: rule.get("IpProtocol", "-1"),
                                    EK.SG_RULE_PORT_RANGE: port_range,
                                    EK.SG_RULE_DIRECTION: "ingress",
                                }),
                            ))

    def _discover_nacls(
        self,
        ec2,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        nodes: list[Node],
        edges: list[Edge],
    ) -> None:
        paginator = ec2.get_paginator("describe_network_acls")
        for page in paginator.paginate():
            for nacl in page.get("NetworkAcls", []):
                nacl_id = nacl["NetworkAclId"]
                vpc_id = nacl.get("VpcId", "unknown")
                nacl_urn = NaclNode.build_urn(account_id, region, vpc_id, nacl_id)
                vpc_urn = VpcNode.build_urn(account_id, region, vpc_id)

                rules = [
                    {
                        "rule_number": entry.get("RuleNumber"),
                        "protocol": entry.get("Protocol"),
                        "rule_action": entry.get("RuleAction"),
                        "egress": entry.get("Egress"),
                        "cidr": entry.get("CidrBlock"),
                    }
                    for entry in nacl.get("Entries", [])
                ]

                nacl_node = NaclNode.create(
                    organization_id=organization_id,
                    urn=nacl_urn,
                    parent_urn=vpc_urn,
                    nacl_id=nacl_id,
                    rules=rules,
                    vpc_id=vpc_id,
                )
                nodes.append(nacl_node)
                edges.append(ContainsEdge.create(organization_id, vpc_urn, nacl_urn))

    @staticmethod
    def _parse_rules(permissions: list[dict]) -> list[dict]:
        """Parse AWS security group rules into a structured format."""
        rules = []
        for perm in permissions:
            base = {
                "protocol": perm.get("IpProtocol", "-1"),
                "from_port": perm.get("FromPort"),
                "to_port": perm.get("ToPort"),
            }
            for ip_range in perm.get("IpRanges", []):
                rules.append({**base, "cidr": ip_range.get("CidrIp"), "type": "ipv4"})
            for ip_range in perm.get("Ipv6Ranges", []):
                rules.append({**base, "cidr": ip_range.get("CidrIpv6"), "type": "ipv6"})
            for sg_ref in perm.get("UserIdGroupPairs", []):
                rules.append({**base, "sg_id": sg_ref.get("GroupId"), "type": "sg_ref"})
        return rules

    @staticmethod
    def _format_port_range(rule: dict) -> str:
        from_port = rule.get("FromPort")
        to_port = rule.get("ToPort")
        if from_port is None and to_port is None:
            return "all"
        if from_port == to_port:
            return str(from_port)
        return f"{from_port}-{to_port}"
