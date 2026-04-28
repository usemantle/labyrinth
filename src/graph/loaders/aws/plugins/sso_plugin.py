"""SSO / Identity Center resource discovery plugin for AwsAccountLoader."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable

import boto3

from src.graph.edges.member_of_edge import MemberOfEdge
from src.graph.graph_models import URN, Edge, Node
from src.graph.loaders.aws.plugins._base import AwsResourcePlugin
from src.graph.nodes.sso_group_node import SsoGroupNode
from src.graph.nodes.sso_user_node import SsoUserNode

logger = logging.getLogger(__name__)


class SsoResourcePlugin(AwsResourcePlugin):
    """Discover AWS IAM Identity Center groups, users, and group memberships."""

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

            instances = sso_admin.list_instances().get("Instances", [])
            if not instances:
                logger.debug("No SSO instances found")
                return nodes, edges

            identity_store_id = instances[0].get("IdentityStoreId")
            if not identity_store_id:
                return nodes, edges

            group_urns_by_id: dict[str, URN] = {}
            group_paginator = identity_store.get_paginator("list_groups")
            for page in group_paginator.paginate(IdentityStoreId=identity_store_id):
                for group in page.get("Groups", []):
                    group_id = group["GroupId"]
                    group_name = group.get("DisplayName", group_id)
                    group_urn = URN(f"urn:aws:sso:{account_id}::group/{group_id}")
                    group_urns_by_id[group_id] = group_urn
                    nodes.append(SsoGroupNode.create(
                        organization_id=organization_id,
                        urn=group_urn,
                        parent_urn=account_urn,
                        group_id=group_id,
                        group_name=group_name,
                    ))

            user_urns_by_id: dict[str, URN] = {}
            user_paginator = identity_store.get_paginator("list_users")
            for page in user_paginator.paginate(IdentityStoreId=identity_store_id):
                for user in page.get("Users", []):
                    user_id = user["UserId"]
                    user_name = user.get("UserName", user_id)
                    primary_email = _primary_email(user.get("Emails", []))
                    external_id = _primary_external_id(user.get("ExternalIds", []))
                    user_urn = URN(f"urn:aws:sso:{account_id}::user/{user_id}")
                    user_urns_by_id[user_id] = user_urn
                    nodes.append(SsoUserNode.create(
                        organization_id=organization_id,
                        urn=user_urn,
                        parent_urn=account_urn,
                        user_id=user_id,
                        user_name=user_name,
                        email=primary_email,
                        external_id=external_id,
                    ))

            membership_paginator = identity_store.get_paginator("list_group_memberships")
            for group_id, group_urn in group_urns_by_id.items():
                try:
                    for page in membership_paginator.paginate(
                        IdentityStoreId=identity_store_id,
                        GroupId=group_id,
                    ):
                        for membership in page.get("GroupMemberships", []):
                            member = membership.get("MemberId", {})
                            user_id = member.get("UserId")
                            if not user_id:
                                continue
                            user_urn = user_urns_by_id.get(user_id)
                            if not user_urn:
                                continue
                            edges.append(MemberOfEdge.create(
                                organization_id=organization_id,
                                from_urn=user_urn,
                                to_urn=group_urn,
                            ))
                except Exception:
                    logger.exception(
                        "Failed to list memberships for group %s", group_id,
                    )

        except Exception:
            logger.exception("Failed to discover SSO resources")

        return nodes, edges


def _primary_email(emails: list[dict]) -> str | None:
    """Return the primary email from an Identity Store user's Emails list, or the first if none flagged primary."""
    if not emails:
        return None
    for entry in emails:
        if entry.get("Primary"):
            value = entry.get("Value")
            if value:
                return value
    first_value = emails[0].get("Value")
    return first_value if first_value else None


def _primary_external_id(external_ids: list[dict]) -> str | None:
    """Return the first ExternalId value (SCIM-provisioned external identifier, e.g. an Okta user id)."""
    if not external_ids:
        return None
    return external_ids[0].get("Id")
