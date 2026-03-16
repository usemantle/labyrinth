"""Stitcher: DNS_RECORD nodes -> LOAD_BALANCER nodes via ResolvesToEdge."""

from __future__ import annotations

import re
import uuid

from src.graph.edges.resolves_to_edge import ResolvesToEdge
from src.graph.graph_models import (
    URN,
    EdgeMetadata,
    EdgeMetadataKey,
    Graph,
    NodeMetadataKey,
    NodeType,
)
from src.graph.stitchers._base import Stitcher


class DnsToLoadBalancerStitcher(Stitcher):
    """Match DNS alias/CNAME values to load balancer DNS names (including API Gateway custom domains)."""

    def stitch(self, organization_id: uuid.UUID, graph: Graph, context: dict) -> Graph:
        NK = NodeMetadataKey
        result = Graph()

        idx = self.index_nodes(graph, types={NodeType.LOAD_BALANCER, NodeType.DNS_RECORD})

        lb_by_dns: dict[str, URN] = {}
        for node in idx.nodes_of_type(NodeType.LOAD_BALANCER):
            dns_name = node.metadata.get(NK.LB_DNS_NAME, "")
            if dns_name:
                lb_by_dns[_normalize_lb_dns(dns_name)] = node.urn

            custom_domains = node.metadata.get(NK.API_GW_CUSTOM_DOMAINS, [])
            if isinstance(custom_domains, list):
                for cd in custom_domains:
                    lb_by_dns[_normalize_lb_dns(cd)] = node.urn

        for dns_node in idx.nodes_of_type(NodeType.DNS_RECORD):
            values = dns_node.metadata.get(NK.DNS_VALUES, [])
            if not isinstance(values, list):
                continue
            for value in values:
                normalized = _normalize_lb_dns(value)
                lb_urn = lb_by_dns.get(normalized)
                if lb_urn:
                    result.edges.append(ResolvesToEdge.create(
                        organization_id, dns_node.urn, lb_urn,
                        metadata=EdgeMetadata({
                            EdgeMetadataKey.DETECTION_METHOD: "dns_alias_match",
                            EdgeMetadataKey.CONFIDENCE: 1.0,
                        }),
                    ))

        return result


def _normalize_lb_dns(dns_name: str) -> str:
    """Normalize a load balancer DNS name for comparison."""
    name = dns_name.lower().strip()
    name = re.sub(r"^https?://", "", name)
    name = name.rstrip("./")
    return name
