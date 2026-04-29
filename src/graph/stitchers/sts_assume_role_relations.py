"""Stitcher: IamRole -> IamRole edges derived from each role's trust policy.

For every IAM role in the graph, this stitcher reads the parsed trust policy
(``NodeMetadataKey.IAM_TRUST_POLICY``) and emits an ``AssumesEdge`` from each
trusted IAM-role principal to the role itself.

The edge means "target trusts source" — it does NOT verify that the source
also has an ``sts:AssumeRole`` permission policy granting access to the
target. A stricter mutual-grant check is intentionally left out and would
belong in a follow-up stitcher.

Conservatism rules:

* Wildcard principals (``"*"``) are skipped — they would explode the edge
  count without conveying useful information.
* Account-root principals (``arn:aws:iam::123456789012:root``) are skipped
  for the same reason; fan-out at the account level belongs in a separate
  stitcher.
* Service principals (``Service: ec2.amazonaws.com``), federated principals
  (``Federated: ...``), and SAML / OIDC principals are skipped — they don't
  correspond to IamRole nodes in the graph.
"""

from __future__ import annotations

import uuid

from src.graph.edges.assumes_edge import AssumesEdge
from src.graph.graph_models import (
    URN,
    EdgeMetadata,
    EdgeMetadataKey,
    Graph,
    NodeMetadataKey,
    NodeType,
)
from src.graph.stitchers._base import Stitcher

NK = NodeMetadataKey
EK = EdgeMetadataKey


def _as_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _action_includes_assume_role(action_value) -> bool:
    actions = _as_list(action_value)
    for action in actions:
        if not isinstance(action, str):
            continue
        if action == "*" or action == "sts:*" or action == "sts:AssumeRole":
            return True
    return False


class StsAssumeRoleRelationsStitcher(Stitcher):
    """Build IamRole -> IamRole edges from each role's trust policy."""

    def stitch(self, organization_id: uuid.UUID, graph: Graph, context: dict) -> Graph:
        result = Graph()

        idx = self.index_nodes(graph, types={NodeType.IAM_ROLE})
        roles = idx.nodes_of_type(NodeType.IAM_ROLE)
        if not roles:
            return result

        # Map ARN -> URN so we can resolve trust-policy principals back to nodes.
        role_urn_by_arn: dict[str, URN] = {}
        for role in roles:
            arn = role.metadata.get(NK.ARN)
            if isinstance(arn, str) and arn:
                role_urn_by_arn[arn] = role.urn

        if not role_urn_by_arn:
            return result

        for target_role in roles:
            trust_policy = target_role.metadata.get(NK.IAM_TRUST_POLICY)
            if not isinstance(trust_policy, dict):
                continue

            statements = _as_list(trust_policy.get("Statement"))
            for stmt in statements:
                if not isinstance(stmt, dict):
                    continue
                if stmt.get("Effect") != "Allow":
                    continue
                if not _action_includes_assume_role(stmt.get("Action")):
                    continue

                principal = stmt.get("Principal")
                if principal == "*":
                    continue
                if not isinstance(principal, dict):
                    continue
                aws_principals = _as_list(principal.get("AWS"))
                condition = stmt.get("Condition")

                for raw in aws_principals:
                    if not isinstance(raw, str):
                        continue
                    if raw == "*":
                        continue
                    if raw.endswith(":root"):
                        # Account-root trust — out of scope for this stitcher.
                        continue
                    source_urn = role_urn_by_arn.get(raw)
                    if source_urn is None:
                        continue
                    if source_urn == target_role.urn:
                        # Skip self-trust.
                        continue

                    edge_meta = EdgeMetadata({
                        EK.ASSUMED_VIA: "iam:trust_policy",
                    })
                    if condition is not None:
                        edge_meta[EK.TRUST_POLICY_CONDITION] = condition
                    result.edges.append(AssumesEdge.create(
                        organization_id=organization_id,
                        from_urn=source_urn,
                        to_urn=target_role.urn,
                        metadata=edge_meta,
                    ))

        return result
