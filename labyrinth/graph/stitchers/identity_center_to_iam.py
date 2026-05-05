"""Stitcher: AWS Identity Center principals -> IAM roles via permission-set assignments.

AWS provides no API that returns the IAM role ARN provisioned by a permission set,
so we infer the link from the deterministic role name AWS generates:
``AWSReservedSSO_<PermissionSetName>_<RandomSuffix>`` placed under path
``/aws-reserved/sso.amazonaws.com/`` in each provisioned account.

Edges emitted (all use AssumesEdge, distinguished by ``assumed_via`` metadata):

* PermissionSet -> IamRole — ``iam:permission_set_role`` — structural link from a
  permission set to the materialised role in a specific account.
* SsoUser/SsoGroup -> IamRole — ``sso:permission_set`` — derived from a
  ``SsoAssignedToEdge`` in the input graph.
* SsoUser -> IamRole (group fan-out) — ``sso:permission_set`` with
  ``via_group`` set to the group's URN — derived by walking ``MemberOfEdge``
  backwards from each Group->Role edge.
"""

from __future__ import annotations

import re
import uuid

from labyrinth.graph.edges.assumes_edge import AssumesEdge
from labyrinth.graph.edges.sso_assigned_to_edge import SsoAssignedToEdge
from labyrinth.graph.graph_models import (
    URN,
    EdgeMetadata,
    EdgeMetadataKey,
    EdgeType,
    Graph,
    NodeMetadataKey,
    NodeType,
)
from labyrinth.graph.stitchers._base import Stitcher

NK = NodeMetadataKey
EK = EdgeMetadataKey

_RESERVED_SSO_ROLE_PATTERN = re.compile(
    r"^AWSReservedSSO_(?P<ps>.+)_[0-9a-fA-F]{8,16}$"
)


def _account_id_from_iam_role_urn(urn: URN) -> str | None:
    """Parse the account id from an IAM role URN of the form ``urn:aws:iam:{account}::role/{name}``."""
    parts = str(urn).split(":")
    if len(parts) < 4 or parts[0] != "urn" or parts[1] != "aws" or parts[2] != "iam":
        return None
    return parts[3] or None


def _ps_name_for_role_match(name: str) -> str:
    """Normalise a permission-set name for matching against an AWSReservedSSO_* role name.

    AWS strips spaces from the permission-set name when generating the role name,
    so we strip spaces from the configured name before comparing.
    """
    return name.replace(" ", "")


class IdentityCenterToIamStitcher(Stitcher):
    """Connect Identity Center principals and permission sets to provisioned IAM roles."""

    def stitch(self, organization_id: uuid.UUID, graph: Graph, context: dict) -> Graph:
        result = Graph()

        idx = self.index_nodes(graph, types={
            NodeType.PERMISSION_SET,
            NodeType.IAM_ROLE,
            NodeType.SSO_USER,
            NodeType.SSO_GROUP,
        })

        permission_sets = idx.nodes_of_type(NodeType.PERMISSION_SET)
        iam_roles = idx.nodes_of_type(NodeType.IAM_ROLE)
        if not permission_sets or not iam_roles:
            return result

        # Step 1: match each permission set to its provisioned IAM roles by name.
        # Build (ps_urn, account_id) -> role_urn for use in step 2.
        ps_account_to_role: dict[tuple[URN, str], URN] = {}
        for ps in permission_sets:
            ps_name = ps.metadata.get(NK.PERMISSION_SET_NAME)
            ps_arn = ps.metadata.get(NK.PERMISSION_SET_ARN)
            if not isinstance(ps_name, str) or not ps_name:
                continue
            normalised_ps_name = _ps_name_for_role_match(ps_name)

            for role in iam_roles:
                role_name = role.metadata.get(NK.ROLE_NAME)
                if not isinstance(role_name, str):
                    continue
                m = _RESERVED_SSO_ROLE_PATTERN.match(role_name)
                if not m:
                    continue
                if m.group("ps") != normalised_ps_name:
                    continue
                account_id = _account_id_from_iam_role_urn(role.urn)
                if not account_id:
                    continue
                ps_account_to_role[(ps.urn, account_id)] = role.urn

                edge_meta = EdgeMetadata({
                    EK.ASSUMED_VIA: "iam:permission_set_role",
                    EK.ACCOUNT_ID: account_id,
                })
                if isinstance(ps_arn, str):
                    edge_meta[EK.PERMISSION_SET_ARN] = ps_arn
                result.edges.append(AssumesEdge.create(
                    organization_id=organization_id,
                    from_urn=ps.urn,
                    to_urn=role.urn,
                    metadata=edge_meta,
                ))

        if not ps_account_to_role:
            return result

        # Step 2: walk SsoAssignedToEdges to emit principal -> role edges.
        ps_arn_by_urn: dict[URN, str] = {}
        for ps in permission_sets:
            arn = ps.metadata.get(NK.PERMISSION_SET_ARN)
            if isinstance(arn, str):
                ps_arn_by_urn[ps.urn] = arn

        principal_type_by_urn: dict[URN, str] = {}
        for sso_user in idx.nodes_of_type(NodeType.SSO_USER):
            principal_type_by_urn[sso_user.urn] = NodeType.SSO_USER
        for sso_group in idx.nodes_of_type(NodeType.SSO_GROUP):
            principal_type_by_urn[sso_group.urn] = NodeType.SSO_GROUP

        group_to_role_edges: list[tuple[URN, URN, str, URN]] = []  # (group_urn, role_urn, account_id, ps_urn)
        for edge in graph.edges:
            if edge.edge_type != EdgeType.SSO_ASSIGNED_TO and not isinstance(edge, SsoAssignedToEdge):
                continue
            assigned_account = edge.metadata.get(EK.ACCOUNT_ID)
            if not isinstance(assigned_account, str):
                continue
            role_urn = ps_account_to_role.get((edge.to_urn, assigned_account))
            if role_urn is None:
                continue

            edge_meta = EdgeMetadata({
                EK.ASSUMED_VIA: "sso:permission_set",
                EK.ACCOUNT_ID: assigned_account,
            })
            ps_arn = ps_arn_by_urn.get(edge.to_urn)
            if ps_arn is not None:
                edge_meta[EK.PERMISSION_SET_ARN] = ps_arn
            result.edges.append(AssumesEdge.create(
                organization_id=organization_id,
                from_urn=edge.from_urn,
                to_urn=role_urn,
                metadata=edge_meta,
            ))

            if principal_type_by_urn.get(edge.from_urn) == NodeType.SSO_GROUP:
                group_to_role_edges.append((edge.from_urn, role_urn, assigned_account, edge.to_urn))

        if not group_to_role_edges:
            return result

        # Step 3: fan out group memberships to user -> role edges.
        # Build group_urn -> [user_urn] from MemberOfEdges already in the graph.
        group_members: dict[URN, list[URN]] = {}
        for edge in graph.edges:
            if edge.edge_type != EdgeType.MEMBER_OF:
                continue
            group_members.setdefault(edge.to_urn, []).append(edge.from_urn)

        for group_urn, role_urn, account_id, ps_urn in group_to_role_edges:
            members = group_members.get(group_urn, [])
            for user_urn in members:
                edge_meta = EdgeMetadata({
                    EK.ASSUMED_VIA: "sso:permission_set",
                    EK.ACCOUNT_ID: account_id,
                    EK.VIA_GROUP: str(group_urn),
                })
                ps_arn = ps_arn_by_urn.get(ps_urn)
                if ps_arn is not None:
                    edge_meta[EK.PERMISSION_SET_ARN] = ps_arn
                result.edges.append(AssumesEdge.create(
                    organization_id=organization_id,
                    from_urn=user_urn,
                    to_urn=role_urn,
                    metadata=edge_meta,
                ))

        return result
