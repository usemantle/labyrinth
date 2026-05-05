"""Stitcher: IamRole -> IamRole edges + insecurity-flag edges from each role's trust policy.

For every IAM role in the graph, this stitcher reads the parsed trust policy
(``NodeMetadataKey.IAM_TRUST_POLICY``) and emits an ``AssumesEdge`` per trusted
principal:

* **IAM-role principals** — emit an edge from the source role to the target
  role with ``assumed_via = "iam:trust_policy"``. This is the primary signal:
  "target trusts source for sts:AssumeRole".

* **Wildcard principal** (``Principal: "*"`` or ``Principal: {"AWS": "*"}``)
  — emit a single edge from ``urn:aws:iam:wildcard::*`` to the role with
  ``assumed_via = "iam:trust_policy_wildcard"`` and
  ``insecure_trust_policy = "wildcard"``. We do not fan out to every role in
  the graph; the synthetic principal URN is enough for downstream rules to
  flag the role.

* **Account-root principal** (``arn:aws:iam::123456789012:root``) — emit an
  edge from a synthetic URN ``urn:aws:iam:{account_id}::root`` to the role
  with ``assumed_via = "iam:trust_policy_account_root"`` and
  ``insecure_trust_policy = "account_root"``. The full set of roles in that
  account would technically be valid sources, but expanding here would
  multiply edges; downstream stitchers can fan out if the account is fully
  scanned.

* **SAML-federated principal** (``Principal: {"Federated": "...:saml-provider/X"}``,
  action ``sts:AssumeRoleWithSAML``) — emit an edge from
  ``urn:aws:iam:{account_id}::saml-provider/{name}`` to the role with
  ``assumed_via = "iam:trust_policy_saml"``. The AWS-internal SSO provider
  (``AWSSSO_*_DO_NOT_DELETE``) is skipped: ``IdentityCenterToIamStitcher``
  already establishes the SsoUser/SsoGroup -> IamRole link structurally for
  those roles, and emitting a duplicate edge would be redundant.

* **OIDC / generic federated** principals are treated like SAML for the
  purposes of edge emission, with ``assumed_via = "iam:trust_policy_federated"``.

* **Service principals** (``Service: ec2.amazonaws.com``) — skipped; they
  represent AWS services, not principals we model as nodes.

The edge means "target trusts source". It does NOT verify that the source
also has an ``sts:AssumeRole`` permission policy granting access — a stricter
mutual-grant check belongs in a follow-up stitcher.
"""

from __future__ import annotations

import re
import uuid

from labyrinth.graph.edges.assumes_edge import AssumesEdge
from labyrinth.graph.graph_models import (
    URN,
    EdgeMetadata,
    EdgeMetadataKey,
    Graph,
    NodeMetadataKey,
    NodeType,
)
from labyrinth.graph.stitchers._base import Stitcher

NK = NodeMetadataKey
EK = EdgeMetadataKey

WILDCARD_PRINCIPAL_URN = URN("urn:aws:iam:wildcard::*")

# AWS Identity Center materialises a SAML provider in each managed account
# whose name follows the pattern ``AWSSSO_<hex>_DO_NOT_DELETE``. The
# trust-policy edge from that provider is redundant with the structural
# SsoUser/SsoGroup -> IamRole link emitted by IdentityCenterToIamStitcher.
_SSO_SAML_PROVIDER_RE = re.compile(r"AWSSSO_[0-9a-fA-F]+_DO_NOT_DELETE$")
_ACCOUNT_ROOT_RE = re.compile(r"^arn:aws:iam::(?P<account>\d{12}):root$")
_SAML_PROVIDER_RE = re.compile(
    r"^arn:aws:iam::(?P<account>\d{12}):saml-provider/(?P<name>.+)$"
)


def _as_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


_ASSUME_ROLE_ACTIONS = {
    "sts:AssumeRole",
    "sts:AssumeRoleWithSAML",
    "sts:AssumeRoleWithWebIdentity",
}


def _action_includes_assume_role(action_value) -> bool:
    actions = _as_list(action_value)
    for action in actions:
        if not isinstance(action, str):
            continue
        if action == "*" or action == "sts:*":
            return True
        if action in _ASSUME_ROLE_ACTIONS:
            return True
    return False


class StsAssumeRoleRelationsStitcher(Stitcher):
    """Build IamRole -> IamRole edges and insecurity-flagging edges from each role's trust policy."""

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

        for target_role in roles:
            trust_policy = target_role.metadata.get(NK.IAM_TRUST_POLICY)
            if not isinstance(trust_policy, dict):
                continue

            for stmt in _as_list(trust_policy.get("Statement")):
                if not isinstance(stmt, dict):
                    continue
                if stmt.get("Effect") != "Allow":
                    continue
                if not _action_includes_assume_role(stmt.get("Action")):
                    continue

                self._handle_statement(
                    organization_id, stmt, target_role.urn, role_urn_by_arn, result,
                )

        return result

    def _handle_statement(
        self,
        organization_id: uuid.UUID,
        stmt: dict,
        target_urn: URN,
        role_urn_by_arn: dict[str, URN],
        result: Graph,
    ) -> None:
        condition = stmt.get("Condition")
        principal = stmt.get("Principal")

        # ``Principal: "*"`` (string form) is a wildcard.
        if principal == "*":
            self._emit_wildcard_edge(organization_id, target_urn, condition, result)
            return

        if not isinstance(principal, dict):
            return

        for raw in _as_list(principal.get("AWS")):
            if not isinstance(raw, str):
                continue
            if raw == "*":
                self._emit_wildcard_edge(organization_id, target_urn, condition, result)
                continue
            account_match = _ACCOUNT_ROOT_RE.match(raw)
            if account_match is not None:
                self._emit_account_root_edge(
                    organization_id, target_urn, account_match.group("account"),
                    condition, result,
                )
                continue
            source_urn = role_urn_by_arn.get(raw)
            if source_urn is None or source_urn == target_urn:
                continue
            self._emit_role_to_role_edge(
                organization_id, source_urn, target_urn, condition, result,
            )

        for raw in _as_list(principal.get("Federated")):
            if not isinstance(raw, str):
                continue
            self._emit_federated_edge(organization_id, target_urn, raw, condition, result)

        # ``Principal.Service`` (e.g. ec2.amazonaws.com) is intentionally skipped.

    # ── Edge emitters ──

    def _emit_role_to_role_edge(
        self,
        organization_id: uuid.UUID,
        source_urn: URN,
        target_urn: URN,
        condition,
        result: Graph,
    ) -> None:
        meta = EdgeMetadata({EK.ASSUMED_VIA: "iam:trust_policy"})
        if condition is not None:
            meta[EK.TRUST_POLICY_CONDITION] = condition
        result.edges.append(AssumesEdge.create(
            organization_id=organization_id,
            from_urn=source_urn,
            to_urn=target_urn,
            metadata=meta,
        ))

    def _emit_wildcard_edge(
        self,
        organization_id: uuid.UUID,
        target_urn: URN,
        condition,
        result: Graph,
    ) -> None:
        meta = EdgeMetadata({
            EK.ASSUMED_VIA: "iam:trust_policy_wildcard",
            EK.INSECURE_TRUST_POLICY: "wildcard",
        })
        if condition is not None:
            meta[EK.TRUST_POLICY_CONDITION] = condition
        result.edges.append(AssumesEdge.create(
            organization_id=organization_id,
            from_urn=WILDCARD_PRINCIPAL_URN,
            to_urn=target_urn,
            metadata=meta,
        ))

    def _emit_account_root_edge(
        self,
        organization_id: uuid.UUID,
        target_urn: URN,
        account_id: str,
        condition,
        result: Graph,
    ) -> None:
        meta = EdgeMetadata({
            EK.ASSUMED_VIA: "iam:trust_policy_account_root",
            EK.INSECURE_TRUST_POLICY: "account_root",
            EK.ACCOUNT_ID: account_id,
        })
        if condition is not None:
            meta[EK.TRUST_POLICY_CONDITION] = condition
        result.edges.append(AssumesEdge.create(
            organization_id=organization_id,
            from_urn=URN(f"urn:aws:iam:{account_id}::root"),
            to_urn=target_urn,
            metadata=meta,
        ))

    def _emit_federated_edge(
        self,
        organization_id: uuid.UUID,
        target_urn: URN,
        raw_principal: str,
        condition,
        result: Graph,
    ) -> None:
        saml_match = _SAML_PROVIDER_RE.match(raw_principal)
        if saml_match is not None and _SSO_SAML_PROVIDER_RE.search(saml_match.group("name")):
            # AWS Identity Center materialises this SAML provider for every
            # SSO-managed account; IdentityCenterToIamStitcher already
            # establishes the SsoUser/SsoGroup -> IamRole link structurally,
            # so emitting a redundant trust-policy edge would just clutter
            # the graph.
            return

        if saml_match is not None:
            account_id = saml_match.group("account")
            name = saml_match.group("name")
            from_urn = URN(f"urn:aws:iam:{account_id}::saml-provider/{name}")
            assumed_via = "iam:trust_policy_saml"
        else:
            # Generic federated principal (OIDC, custom IdP, etc.). We don't
            # parse out an account or name reliably, so emit from the raw ARN
            # rendered as a URN-shaped synthetic.
            from_urn = URN(f"urn:aws:iam:federated::{raw_principal}")
            assumed_via = "iam:trust_policy_federated"

        meta = EdgeMetadata({
            EK.ASSUMED_VIA: assumed_via,
            EK.SAML_PROVIDER_ARN: raw_principal,
        })
        if condition is not None:
            meta[EK.TRUST_POLICY_CONDITION] = condition
        result.edges.append(AssumesEdge.create(
            organization_id=organization_id,
            from_urn=from_urn,
            to_urn=target_urn,
            metadata=meta,
        ))
