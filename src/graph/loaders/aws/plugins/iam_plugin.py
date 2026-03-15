"""IAM resource discovery plugin for AwsAccountLoader."""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Callable

import boto3

from src.graph.edges.attaches_edge import AttachesEdge
from src.graph.graph_models import URN, Edge, EdgeMetadata, EdgeMetadataKey, Node
from src.graph.loaders.aws.plugins._base import AwsResourcePlugin
from src.graph.nodes.iam_policy_node import IamPolicyNode
from src.graph.nodes.iam_role_node import IamRoleNode
from src.graph.nodes.iam_user_node import IamUserNode

logger = logging.getLogger(__name__)

EK = EdgeMetadataKey


class IamResourcePlugin(AwsResourcePlugin):
    """Discover IAM roles, users, and policies."""

    def service_name(self) -> str:
        return "iam"

    def discover(
        self,
        session: boto3.Session,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        build_urn: Callable[..., URN],
    ) -> tuple[list[Node], list[Edge]]:
        iam = session.client("iam")
        nodes: list[Node] = []
        edges: list[Edge] = []

        self._discover_roles(iam, account_id, organization_id, account_urn, nodes, edges)
        self._discover_users(iam, account_id, organization_id, account_urn, nodes, edges)
        self._discover_policies(iam, account_id, organization_id, account_urn, nodes, edges)

        return nodes, edges

    def _discover_roles(
        self,
        iam,
        account_id: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        nodes: list[Node],
        edges: list[Edge],
    ) -> None:
        try:
            paginator = iam.get_paginator("list_roles")
            for page in paginator.paginate():
                for role in page.get("Roles", []):
                    role_name = role["RoleName"]
                    role_urn = URN(f"urn:aws:iam:{account_id}::role/{role_name}")

                    trust_policy = role.get("AssumeRolePolicyDocument")
                    if isinstance(trust_policy, str):
                        trust_policy = json.loads(trust_policy)

                    node = IamRoleNode.create(
                        organization_id=organization_id,
                        urn=role_urn,
                        parent_urn=account_urn,
                        role_name=role_name,
                        trust_policy=trust_policy,
                        arn=role.get("Arn"),
                    )
                    nodes.append(node)

                    # Discover attached policies for this role
                    self._attach_role_policies(
                        iam, role_name, account_id, organization_id, role_urn, edges,
                    )
        except Exception:
            logger.exception("Failed to list IAM roles")

    def _attach_role_policies(
        self,
        iam,
        role_name: str,
        account_id: str,
        organization_id: uuid.UUID,
        role_urn: URN,
        edges: list[Edge],
    ) -> None:
        try:
            paginator = iam.get_paginator("list_attached_role_policies")
            for page in paginator.paginate(RoleName=role_name):
                for policy in page.get("AttachedPolicies", []):
                    policy_name = policy["PolicyName"]
                    policy_arn_str = policy.get("PolicyArn", "")

                    # Use the policy ARN to determine if it's AWS-managed or customer-managed
                    if policy_arn_str.startswith("arn:aws:iam::aws:"):
                        policy_urn = URN(f"urn:aws:iam:{account_id}::policy/aws/{policy_name}")
                    else:
                        policy_urn = URN(f"urn:aws:iam:{account_id}::policy/{policy_name}")

                    edges.append(AttachesEdge.create(
                        organization_id, policy_urn, role_urn,
                        metadata=EdgeMetadata({EK.PRIVILEGE: "attached"}),
                    ))
        except Exception:
            logger.debug("Failed to list attached policies for role %s", role_name)

    def _discover_users(
        self,
        iam,
        account_id: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        nodes: list[Node],
        edges: list[Edge],
    ) -> None:
        try:
            paginator = iam.get_paginator("list_users")
            for page in paginator.paginate():
                for user in page.get("Users", []):
                    user_name = user["UserName"]
                    user_urn = URN(f"urn:aws:iam:{account_id}::user/{user_name}")

                    # Get access keys
                    access_keys = self._get_access_keys(iam, user_name)

                    # Get MFA devices
                    mfa_enabled = self._check_mfa(iam, user_name)

                    # Last activity from PasswordLastUsed
                    last_activity = None
                    if user.get("PasswordLastUsed"):
                        last_activity = user["PasswordLastUsed"].isoformat()

                    node = IamUserNode.create(
                        organization_id=organization_id,
                        urn=user_urn,
                        parent_urn=account_urn,
                        user_name=user_name,
                        access_keys=access_keys or None,
                        mfa_enabled=mfa_enabled,
                        last_activity=last_activity,
                        arn=user.get("Arn"),
                    )
                    nodes.append(node)

                    # Discover attached policies for this user
                    self._attach_user_policies(
                        iam, user_name, account_id, organization_id, user_urn, edges,
                    )
        except Exception:
            logger.exception("Failed to list IAM users")

    def _get_access_keys(self, iam, user_name: str) -> list[dict]:
        try:
            resp = iam.list_access_keys(UserName=user_name)
            return [
                {
                    "id": key["AccessKeyId"],
                    "status": key["Status"],
                    "created": key.get("CreateDate", "").isoformat()
                    if hasattr(key.get("CreateDate", ""), "isoformat")
                    else str(key.get("CreateDate", "")),
                }
                for key in resp.get("AccessKeyMetadata", [])
            ]
        except Exception:
            return []

    def _check_mfa(self, iam, user_name: str) -> bool:
        try:
            resp = iam.list_mfa_devices(UserName=user_name)
            return len(resp.get("MFADevices", [])) > 0
        except Exception:
            return False

    def _attach_user_policies(
        self,
        iam,
        user_name: str,
        account_id: str,
        organization_id: uuid.UUID,
        user_urn: URN,
        edges: list[Edge],
    ) -> None:
        try:
            paginator = iam.get_paginator("list_attached_user_policies")
            for page in paginator.paginate(UserName=user_name):
                for policy in page.get("AttachedPolicies", []):
                    policy_name = policy["PolicyName"]
                    policy_arn_str = policy.get("PolicyArn", "")

                    if policy_arn_str.startswith("arn:aws:iam::aws:"):
                        policy_urn = URN(f"urn:aws:iam:{account_id}::policy/aws/{policy_name}")
                    else:
                        policy_urn = URN(f"urn:aws:iam:{account_id}::policy/{policy_name}")

                    edges.append(AttachesEdge.create(
                        organization_id, policy_urn, user_urn,
                        metadata=EdgeMetadata({EK.PRIVILEGE: "attached"}),
                    ))
        except Exception:
            logger.debug("Failed to list attached policies for user %s", user_name)

    def _discover_policies(
        self,
        iam,
        account_id: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        nodes: list[Node],
        edges: list[Edge],
    ) -> None:
        try:
            paginator = iam.get_paginator("list_policies")
            for page in paginator.paginate(Scope="Local"):
                for policy in page.get("Policies", []):
                    policy_name = policy["PolicyName"]
                    policy_arn_str = policy.get("Arn", "")
                    policy_urn = URN(f"urn:aws:iam:{account_id}::policy/{policy_name}")

                    # Get policy document for the default version
                    policy_doc = self._get_policy_document(
                        iam, policy_arn_str, policy.get("DefaultVersionId", "v1"),
                    )

                    node = IamPolicyNode.create(
                        organization_id=organization_id,
                        urn=policy_urn,
                        parent_urn=account_urn,
                        policy_name=policy_name,
                        policy_arn=policy_arn_str,
                        policy_document=policy_doc,
                    )
                    nodes.append(node)
        except Exception:
            logger.exception("Failed to list IAM policies")

    def _get_policy_document(
        self, iam, policy_arn: str, version_id: str,
    ) -> dict | None:
        try:
            resp = iam.get_policy_version(
                PolicyArn=policy_arn, VersionId=version_id,
            )
            doc = resp.get("PolicyVersion", {}).get("Document")
            if isinstance(doc, str):
                return json.loads(doc)
            return doc
        except Exception:
            return None
