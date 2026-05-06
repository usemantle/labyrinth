"""SsoAssignedToEdge — an Identity Center principal assigned a permission set in an account."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from labyrinth.graph.graph_models import EDGE_NAMESPACE, URN, Edge, EdgeMetadata, EdgeType


@dataclass
class SsoAssignedToEdge(Edge):
    """An SSO user or group assigned a permission set in a specific AWS account.

    One assignment exists per (principal, permission_set, account) tuple, so the
    same principal+permission-set pair across N accounts produces N edges. The
    target account is recorded in metadata under ``account_id``.
    """

    edge_type: str = EdgeType.SSO_ASSIGNED_TO

    @classmethod
    def create(
        cls,
        organization_id: uuid.UUID,
        from_urn: URN,
        to_urn: URN,
        *,
        account_id: str,
        metadata: EdgeMetadata | None = None,
    ) -> SsoAssignedToEdge:
        edge_uuid = uuid.uuid5(
            EDGE_NAMESPACE,
            f"{from_urn}:{to_urn}:{account_id}:SSO_ASSIGNED_TO",
        )
        meta = metadata or EdgeMetadata()
        meta["account_id"] = account_id
        return cls(
            uuid=edge_uuid,
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=to_urn,
            metadata=meta,
        )
