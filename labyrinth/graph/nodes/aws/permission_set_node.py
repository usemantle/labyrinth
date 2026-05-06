"""PermissionSetNode — an AWS IAM Identity Center permission set."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import ClassVar

from labyrinth.graph.edges.assumes_edge import AssumesEdge
from labyrinth.graph.edges.contains_edge import ContainsEdge
from labyrinth.graph.edges.sso_assigned_to_edge import SsoAssignedToEdge
from labyrinth.graph.graph_models import URN, Node, NodeMetadata, NodeMetadataKey, NodeType

NK = NodeMetadataKey


@dataclass
class PermissionSetNode(Node):
    """An AWS IAM Identity Center permission set.

    A permission set defines a named bundle of policies that can be provisioned
    into one or more AWS accounts. When a principal is assigned a permission
    set in an account, IAM Identity Center materialises a role named
    ``AWSReservedSSO_<PermissionSetName>_<RandomSuffix>`` under
    ``/aws-reserved/sso.amazonaws.com/`` in that account.
    """

    node_type: str = NodeType.PERMISSION_SET

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset({
        AssumesEdge,
    })
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset({
        ContainsEdge,
        SsoAssignedToEdge,
    })

    @staticmethod
    def build_urn(account_id: str, permission_set_id: str) -> URN:
        return URN(f"urn:aws:sso:{account_id}::permission-set/{permission_set_id}")

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        urn: URN,
        parent_urn: URN | None = None,
        *,
        permission_set_arn: str,
        instance_arn: str,
        name: str,
        description: str | None = None,
        session_duration: str | None = None,
    ) -> PermissionSetNode:
        meta = NodeMetadata({
            NK.PERMISSION_SET_NAME: name,
            NK.PERMISSION_SET_ARN: permission_set_arn,
            NK.PERMISSION_SET_INSTANCE_ARN: instance_arn,
        })
        if description is not None:
            meta[NK.PERMISSION_SET_DESCRIPTION] = description
        if session_duration is not None:
            meta[NK.PERMISSION_SET_SESSION_DURATION] = session_duration
        return cls(
            organization_id=organization_id,
            urn=urn,
            parent_urn=parent_urn,
            metadata=meta,
        )
