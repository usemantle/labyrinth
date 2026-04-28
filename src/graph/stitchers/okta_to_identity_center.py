"""Stitcher: Okta PersonNode -> AWS SsoUserNode via OktaMapsToEdge."""

from __future__ import annotations

import uuid

from src.graph.edges.okta_edges import OktaMapsToEdge
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


class OktaToIdentityCenterStitcher(Stitcher):
    """Match Okta PersonNodes to AWS Identity Center SsoUserNodes.

    Match priority:
      1. SsoUser.external_id == Person.okta_id
         (set when Okta is the SCIM provisioner for AWS Identity Center; strongest signal).
      2. SsoUser.email == Person.email (case-insensitive).

    Emits OktaMapsToEdge(Person -> SsoUser) with provenance metadata.
    """

    def stitch(self, organization_id: uuid.UUID, graph: Graph, context: dict) -> Graph:
        result = Graph()

        idx = self.index_nodes(graph, types={NodeType.PERSON, NodeType.SSO_USER})

        sso_by_external_id: dict[str, URN] = {}
        sso_by_email: dict[str, URN] = {}
        for sso in idx.nodes_of_type(NodeType.SSO_USER):
            external_id = sso.metadata.get(NK.SSO_USER_EXTERNAL_ID)
            if isinstance(external_id, str) and external_id:
                sso_by_external_id.setdefault(external_id, sso.urn)
            email = sso.metadata.get(NK.SSO_USER_EMAIL)
            if isinstance(email, str) and email:
                sso_by_email.setdefault(email.lower(), sso.urn)

        for person in idx.nodes_of_type(NodeType.PERSON):
            okta_id = person.metadata.get(NK.PERSON_OKTA_ID)
            sso_urn: URN | None = None
            match_key = ""
            match_value = ""

            if isinstance(okta_id, str) and okta_id in sso_by_external_id:
                sso_urn = sso_by_external_id[okta_id]
                match_key = "externalId"
                match_value = okta_id
            else:
                email = person.metadata.get(NK.PERSON_EMAIL)
                if isinstance(email, str) and email:
                    candidate = sso_by_email.get(email.lower())
                    if candidate is not None:
                        sso_urn = candidate
                        match_key = "email"
                        match_value = email.lower()

            if sso_urn is None:
                continue

            confidence = 1.0 if match_key == "externalId" else 0.85
            result.edges.append(OktaMapsToEdge.create(
                organization_id=organization_id,
                from_urn=person.urn,
                to_urn=sso_urn,
                metadata=EdgeMetadata({
                    EK.DETECTION_METHOD: "okta_to_identity_center",
                    EK.MATCH_KEY: match_key,
                    EK.MATCH_VALUE: match_value,
                    EK.CONFIDENCE: confidence,
                }),
            ))

        return result
