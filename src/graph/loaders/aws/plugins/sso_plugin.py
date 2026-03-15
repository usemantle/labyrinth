"""SSO / Identity Center resource discovery plugin for AwsAccountLoader."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable

import boto3

from src.graph.graph_models import URN, Edge, Node
from src.graph.loaders.aws.plugins._base import AwsResourcePlugin
from src.graph.nodes.sso_group_node import SsoGroupNode

logger = logging.getLogger(__name__)


class SsoResourcePlugin(AwsResourcePlugin):
    """Discover AWS SSO / Identity Center groups."""

    def service_name(self) -> str:
        return "sso"

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

        try:
            identity_store = session.client("identitystore", region_name=region)
            sso_admin = session.client("sso-admin", region_name=region)

            # Find the SSO instance
            instances = sso_admin.list_instances().get("Instances", [])
            if not instances:
                logger.debug("No SSO instances found")
                return nodes, edges

            identity_store_id = instances[0].get("IdentityStoreId")
            if not identity_store_id:
                return nodes, edges

            # List groups
            paginator = identity_store.get_paginator("list_groups")
            for page in paginator.paginate(IdentityStoreId=identity_store_id):
                for group in page.get("Groups", []):
                    group_id = group["GroupId"]
                    group_name = group.get("DisplayName", group_id)

                    group_urn = URN(f"urn:aws:sso:{account_id}::group/{group_id}")

                    node = SsoGroupNode.create(
                        organization_id=organization_id,
                        urn=group_urn,
                        parent_urn=account_urn,
                        group_id=group_id,
                        group_name=group_name,
                    )
                    nodes.append(node)

        except Exception:
            logger.exception("Failed to discover SSO groups")

        return nodes, edges
