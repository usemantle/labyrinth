"""Tests for ApiGatewayResourcePlugin."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from labyrinth.graph.graph_models import URN, NodeMetadataKey
from labyrinth.graph.loaders.aws.plugins.apigateway_plugin import ApiGatewayResourcePlugin

ORG_ID = uuid.uuid4()
NK = NodeMetadataKey
ACCOUNT_URN = URN("urn:aws:account:123456789012:us-east-1:root")


def _make_session(http_apis=None, rest_apis=None):
    session = MagicMock()

    apigwv2 = MagicMock()
    apigwv2.get_apis.return_value = {
        "Items": http_apis or [],
    }

    apigw = MagicMock()
    rest_paginator = MagicMock()
    rest_paginator.paginate.return_value = [{"items": rest_apis or []}]
    apigw.get_paginator.return_value = rest_paginator

    def client_factory(name, **kwargs):
        if name == "apigatewayv2":
            return apigwv2
        if name == "apigateway":
            return apigw
        return MagicMock()

    session.client = client_factory
    return session


class TestApiGatewayResourcePlugin:
    def test_service_name(self):
        assert ApiGatewayResourcePlugin().service_name() == "apigateway"

    def test_discover_http_api(self):
        http_apis = [{
            "ApiId": "abc123",
            "Name": "my-http-api",
            "ApiEndpoint": "https://abc123.execute-api.us-east-1.amazonaws.com",
            "ProtocolType": "HTTP",
        }]

        session = _make_session(http_apis=http_apis)
        plugin = ApiGatewayResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 1
        assert nodes[0].node_type == "api_gateway"
        assert nodes[0].metadata[NK.LB_TYPE] == "http"
        assert nodes[0].metadata[NK.LB_SCHEME] == "internet-facing"
        assert nodes[0].metadata[NK.LB_DNS_NAME] == "https://abc123.execute-api.us-east-1.amazonaws.com"

    def test_discover_rest_api(self):
        rest_apis = [{
            "id": "xyz789",
            "name": "my-rest-api",
            "endpointConfiguration": {"types": ["REGIONAL"]},
        }]

        session = _make_session(rest_apis=rest_apis)
        plugin = ApiGatewayResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 1
        assert nodes[0].node_type == "api_gateway"
        assert nodes[0].metadata[NK.LB_TYPE] == "rest"
        assert nodes[0].metadata[NK.LB_SCHEME] == "internet-facing"
        assert "xyz789.execute-api.us-east-1.amazonaws.com" in nodes[0].metadata[NK.LB_DNS_NAME]

    def test_discover_private_rest_api(self):
        rest_apis = [{
            "id": "priv001",
            "name": "private-api",
            "endpointConfiguration": {"types": ["PRIVATE"]},
        }]

        session = _make_session(rest_apis=rest_apis)
        plugin = ApiGatewayResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 1
        assert nodes[0].metadata[NK.LB_SCHEME] == "internal"

    def test_discover_no_apis(self):
        session = _make_session()
        plugin = ApiGatewayResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 0
        assert len(edges) == 0

    def test_discover_both_http_and_rest(self):
        http_apis = [{
            "ApiId": "http1",
            "Name": "http-api",
            "ApiEndpoint": "https://http1.execute-api.us-east-1.amazonaws.com",
            "ProtocolType": "HTTP",
        }]
        rest_apis = [{
            "id": "rest1",
            "name": "rest-api",
            "endpointConfiguration": {"types": ["EDGE"]},
        }]

        session = _make_session(http_apis=http_apis, rest_apis=rest_apis)
        plugin = ApiGatewayResourcePlugin()

        nodes, edges = plugin.discover(
            session=session, account_id="123456789012", region="us-east-1",
            organization_id=ORG_ID, account_urn=ACCOUNT_URN,
            build_urn=lambda *s: URN("urn:test:test:test:test:" + "/".join(s)),
        )

        assert len(nodes) == 2
        lb_types = {n.metadata[NK.LB_TYPE] for n in nodes}
        assert "http" in lb_types
        assert "rest" in lb_types
