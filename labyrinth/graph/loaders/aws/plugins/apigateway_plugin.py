"""API Gateway resource discovery plugin for AwsAccountLoader."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable

import boto3

from labyrinth.graph.graph_models import URN, Edge, Node, NodeMetadataKey
from labyrinth.graph.loaders.aws.plugins._base import AwsResourcePlugin
from labyrinth.graph.nodes.api_gateway_node import ApiGatewayNode

NK = NodeMetadataKey

logger = logging.getLogger(__name__)


class ApiGatewayResourcePlugin(AwsResourcePlugin):
    """Discover API Gateway (REST and HTTP) endpoints."""

    def service_name(self) -> str:
        return "apigateway"

    def discover(
        self,
        session: boto3.Session,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        build_urn: Callable[..., URN],
    ) -> tuple[list[Node], list[Edge]]:
        nodes: list[Node] = []
        edges: list[Edge] = []

        # HTTP APIs (API Gateway v2)
        self._discover_http_apis(
            session, region, organization_id, account_urn,
            nodes, edges,
        )

        # REST APIs (API Gateway v1)
        self._discover_rest_apis(
            session, region, organization_id, account_urn,
            nodes, edges,
        )

        logger.info("API Gateway: discovered %d endpoints", len(nodes))
        return nodes, edges

    def _discover_http_apis(
        self,
        session: boto3.Session,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        nodes: list[Node],
        edges: list[Edge],
    ) -> None:
        try:
            apigwv2 = session.client("apigatewayv2", region_name=region)
            apis = self._get_http_apis(apigwv2)
        except Exception:
            logger.exception("Failed to list HTTP APIs")
            return

        for api in apis:
            api_id = api["ApiId"]
            endpoint = api.get("ApiEndpoint", "")
            protocol_type = api.get("ProtocolType", "HTTP")

            urn = ApiGatewayNode.build_urn(region, api_id)

            api_gw_type = "http" if protocol_type == "HTTP" else "websocket"

            node = ApiGatewayNode.create(
                organization_id=organization_id,
                urn=urn,
                parent_urn=account_urn,
                api_gw_type=api_gw_type,
                scheme="internet-facing",
                dns_name=endpoint,
                api_id=api.get("ApiId"),
                endpoint_type=protocol_type,
            )

            # Discover integrations (e.g. VPC_LINK to ALB)
            integration_uris = self._get_integration_uris(apigwv2, api_id)
            if integration_uris:
                node.metadata[NK.API_GW_INTEGRATION_URIS] = integration_uris

            # Discover custom domain mappings
            custom_domains = self._get_custom_domains(apigwv2, api_id)
            if custom_domains:
                node.metadata[NK.API_GW_CUSTOM_DOMAINS] = custom_domains

            nodes.append(node)

    def _discover_rest_apis(
        self,
        session: boto3.Session,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        nodes: list[Node],
        edges: list[Edge],
    ) -> None:
        try:
            apigw = session.client("apigateway", region_name=region)
            apis = self._get_rest_apis(apigw)
        except Exception:
            logger.exception("Failed to list REST APIs")
            return

        for api in apis:
            api_id = api["id"]
            endpoint_config = api.get("endpointConfiguration", {})
            endpoint_types = endpoint_config.get("types", [])

            # Determine scheme
            scheme = "internal" if "PRIVATE" in endpoint_types else "internet-facing"
            endpoint_type = endpoint_types[0] if endpoint_types else "EDGE"

            # REST API DNS name follows a predictable pattern
            dns_name = f"{api_id}.execute-api.{region}.amazonaws.com"

            urn = ApiGatewayNode.build_urn(region, api_id)

            node = ApiGatewayNode.create(
                organization_id=organization_id,
                urn=urn,
                parent_urn=account_urn,
                api_gw_type="rest",
                scheme=scheme,
                dns_name=dns_name,
                api_id=api_id,
                endpoint_type=endpoint_type,
            )
            nodes.append(node)

    def _get_http_apis(self, apigwv2) -> list[dict]:
        apis: list[dict] = []
        next_token = None
        while True:
            kwargs = {}
            if next_token:
                kwargs["NextToken"] = next_token
            resp = apigwv2.get_apis(**kwargs)
            apis.extend(resp.get("Items", []))
            next_token = resp.get("NextToken")
            if not next_token:
                break
        return apis

    def _get_rest_apis(self, apigw) -> list[dict]:
        apis: list[dict] = []
        paginator = apigw.get_paginator("get_rest_apis")
        for page in paginator.paginate():
            apis.extend(page.get("items", []))
        return apis

    def _get_integration_uris(self, apigwv2, api_id: str) -> list[str]:
        """Fetch integration URIs (e.g. ALB listener ARNs) for an HTTP API."""
        try:
            resp = apigwv2.get_integrations(ApiId=api_id)
            uris: list[str] = []
            for integration in resp.get("Items", []):
                uri = integration.get("IntegrationUri", "")
                if uri:
                    uris.append(uri)
            return uris
        except Exception:
            logger.debug("Failed to get integrations for API %s", api_id)
            return []

    def _get_custom_domains(self, apigwv2, api_id: str) -> list[str]:
        """Fetch custom domain names mapped to this API."""
        try:
            resp = apigwv2.get_domain_names()
            domains: list[str] = []
            for domain in resp.get("Items", []):
                domain_name = domain.get("DomainName", "")
                if not domain_name:
                    continue
                # Check if this domain is mapped to our API
                try:
                    mappings = apigwv2.get_api_mappings(DomainName=domain_name)
                    for mapping in mappings.get("Items", []):
                        if mapping.get("ApiId") == api_id:
                            # Get the target DNS name from the domain configuration
                            config = domain.get("DomainNameConfigurations", [{}])
                            target_dns = config[0].get(
                                "ApiGatewayDomainName", "",
                            ) if config else ""
                            if target_dns:
                                domains.append(target_dns)
                            # Also store the custom domain name itself
                            domains.append(domain_name)
                except Exception:
                    logger.debug(
                        "Failed to get API mappings for domain %s",
                        domain_name,
                    )
            return domains
        except Exception:
            logger.debug("Failed to get custom domains for API %s", api_id)
            return []
