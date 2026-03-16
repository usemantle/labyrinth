"""Route53 resource discovery plugin for AwsAccountLoader."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable

import boto3

from src.graph.graph_models import URN, Edge, Node
from src.graph.loaders.aws.plugins._base import AwsResourcePlugin
from src.graph.nodes.dns_record_node import DnsRecordNode

logger = logging.getLogger(__name__)

_SKIP_TYPES = {"SOA", "NS"}


class Route53ResourcePlugin(AwsResourcePlugin):
    """Discover Route53 hosted zones and DNS records."""

    def service_name(self) -> str:
        return "route53"

    def discover(
        self,
        session: boto3.Session,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        build_urn: Callable[..., URN],
    ) -> tuple[list[Node], list[Edge]]:
        route53 = session.client("route53", region_name=region)
        nodes: list[Node] = []
        edges: list[Edge] = []

        try:
            zones = self._list_hosted_zones(route53)
        except Exception:
            logger.exception("Failed to list Route53 hosted zones")
            return nodes, edges

        for zone in zones:
            zone_id = zone["Id"].split("/")[-1]
            zone_name = zone["Name"].rstrip(".")
            zone_private = zone.get("Config", {}).get("PrivateZone", False)

            try:
                records = self._list_records(route53, zone["Id"])
            except Exception:
                logger.exception("Failed to list records for zone %s", zone_id)
                continue

            for record in records:
                record_type = record.get("Type", "")
                if record_type in _SKIP_TYPES:
                    continue

                record_name = record.get("Name", "").rstrip(".")
                ttl = record.get("TTL")

                # Collect values (resource records or alias target)
                values: list[str] = []
                if "ResourceRecords" in record:
                    values = [rr["Value"] for rr in record["ResourceRecords"]]
                elif "AliasTarget" in record:
                    values = [record["AliasTarget"]["DNSName"].rstrip(".")]

                urn = URN(
                    f"urn:aws:route53:{account_id}::{zone_id}/{record_name}/{record_type}"
                )

                node = DnsRecordNode.create(
                    organization_id=organization_id,
                    urn=urn,
                    parent_urn=account_urn,
                    record_name=record_name,
                    record_type=record_type,
                    zone_name=zone_name,
                    zone_private=zone_private,
                    zone_id=zone_id,
                    ttl=ttl,
                    values=values,
                )
                nodes.append(node)

        logger.info("Route53: discovered %d DNS records", len(nodes))
        return nodes, edges

    def _list_hosted_zones(self, route53) -> list[dict]:
        zones: list[dict] = []
        paginator = route53.get_paginator("list_hosted_zones")
        for page in paginator.paginate():
            zones.extend(page.get("HostedZones", []))
        return zones

    def _list_records(self, route53, zone_id: str) -> list[dict]:
        records: list[dict] = []
        paginator = route53.get_paginator("list_resource_record_sets")
        for page in paginator.paginate(HostedZoneId=zone_id):
            records.extend(page.get("ResourceRecordSets", []))
        return records
