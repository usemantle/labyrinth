"""OktaLoader — discovers Persons, Groups, Applications, and IdP edges from an Okta organization."""

from __future__ import annotations

import logging
import re
import uuid
from typing import Any

import httpx

from src.graph.credentials import CredentialBase, OktaTokenCredential
from src.graph.edges.idp_assigned_to_edge import IdpAssignedToEdge
from src.graph.edges.idp_part_of_edge import IdpPartOfEdge
from src.graph.edges.idp_pushes_to_edge import IdpPushesToEdge
from src.graph.graph_models import URN, Edge, Node
from src.graph.loaders.loader import ConceptLoader, URNComponent
from src.graph.nodes.application_node import ApplicationNode
from src.graph.nodes.group_node import GroupNode
from src.graph.nodes.person_node import PersonNode

logger = logging.getLogger(__name__)

_PAGE_LIMIT = 200
_LINK_NEXT_RE = re.compile(r'<([^>]+)>\s*;\s*rel="next"')


class OktaLoader(ConceptLoader):
    """Loader for an Okta organization.

    Discovers users (PersonNode), groups (GroupNode), applications (ApplicationNode),
    and IdP edges (IDP:PART_OF, IDP:ASSIGNED_TO, IDP:PUSHES_TO) by paging through the
    Okta Core API with an SSWS token.
    """

    def __init__(
        self,
        organization_id: uuid.UUID,
        domain: str,
        api_token: str,
    ):
        super().__init__(organization_id)
        # Strip any protocol prefix and trailing slash the user may have entered.
        self._domain = domain.removeprefix("https://").removeprefix("http://").rstrip("/")
        self._api_token = api_token
        self._base_url = f"https://{self._domain}/api/v1"

    def build_urn(self, *path_segments: str) -> URN:
        path = "/".join(path_segments)
        return URN(f"urn:okta:idp:{self._domain}::{path}")

    @classmethod
    def display_name(cls) -> str:
        return "Okta"

    @classmethod
    def urn_components(cls) -> list[URNComponent]:
        return [
            URNComponent(
                name="domain",
                description="Okta domain (e.g. yourorg.okta.com)",
            ),
        ]

    @classmethod
    def credential_type(cls) -> type[CredentialBase]:
        return OktaTokenCredential

    @classmethod
    def build_target_urn(cls, **components: str) -> URN:
        return URN(f"urn:okta:idp:{components['domain']}::root")

    @classmethod
    def from_target_config(
        cls,
        project_id: uuid.UUID,
        urn: URN,
        credentials: dict,
        **kwargs: Any,
    ) -> tuple[OktaLoader, str]:
        domain = urn.account
        return cls(project_id, domain, credentials["api_token"]), str(urn)

    def load(self, resource: str) -> tuple[list[Node], list[Edge]]:
        nodes: list[Node] = []
        edges: list[Edge] = []

        with httpx.Client(
            base_url=self._base_url,
            headers={
                "Authorization": f"SSWS {self._api_token}",
                "Accept": "application/json",
                "User-Agent": "labyrinth-okta-loader",
            },
            timeout=30.0,
        ) as client:
            user_urn_by_id = self._load_users(client, nodes)
            group_urn_by_id = self._load_groups(client, nodes)
            app_urn_by_id = self._load_applications(client, nodes)

            for group_id, group_urn in group_urn_by_id.items():
                self._load_group_memberships(
                    client, group_id, group_urn, user_urn_by_id, edges,
                )

            for app_id, app_urn in app_urn_by_id.items():
                self._load_app_user_assignments(
                    client, app_id, app_urn, user_urn_by_id, edges,
                )
                self._load_app_group_assignments(
                    client, app_id, app_urn, group_urn_by_id, edges,
                )
                self._load_app_group_push(
                    client, app_id, app_urn, group_urn_by_id, edges,
                )

        return nodes, edges

    # ── User / Group / Application discovery ──────────────────────────────

    def _load_users(
        self, client: httpx.Client, nodes: list[Node],
    ) -> dict[str, URN]:
        result: dict[str, URN] = {}
        for user in self._paginate(client, "/users", {"limit": _PAGE_LIMIT}):
            user_id = user["id"]
            profile = user.get("profile") or {}
            urn = self.build_urn("user", user_id)
            result[user_id] = urn
            nodes.append(PersonNode.create(
                organization_id=self.organization_id,
                urn=urn,
                okta_id=user_id,
                email=profile.get("email"),
                login=profile.get("login"),
                status=user.get("status"),
                display_name=_full_name(profile),
            ))
        logger.info("Discovered %d Okta users", len(result))
        return result

    def _load_groups(
        self, client: httpx.Client, nodes: list[Node],
    ) -> dict[str, URN]:
        result: dict[str, URN] = {}
        for group in self._paginate(client, "/groups", {"limit": _PAGE_LIMIT}):
            group_id = group["id"]
            profile = group.get("profile") or {}
            urn = self.build_urn("group", group_id)
            result[group_id] = urn
            nodes.append(GroupNode.create(
                organization_id=self.organization_id,
                urn=urn,
                okta_id=group_id,
                name=profile.get("name"),
                description=profile.get("description"),
            ))
        logger.info("Discovered %d Okta groups", len(result))
        return result

    def _load_applications(
        self, client: httpx.Client, nodes: list[Node],
    ) -> dict[str, URN]:
        result: dict[str, URN] = {}
        for app in self._paginate(client, "/apps", {"limit": _PAGE_LIMIT}):
            app_id = app["id"]
            urn = self.build_urn("app", app_id)
            result[app_id] = urn
            nodes.append(ApplicationNode.create(
                organization_id=self.organization_id,
                urn=urn,
                okta_id=app_id,
                name=app.get("name"),
                label=app.get("label"),
                sign_on_mode=app.get("signOnMode"),
                status=app.get("status"),
            ))
        logger.info("Discovered %d Okta applications", len(result))
        return result

    # ── Edge discovery ────────────────────────────────────────────────────

    def _load_group_memberships(
        self,
        client: httpx.Client,
        group_id: str,
        group_urn: URN,
        user_urn_by_id: dict[str, URN],
        edges: list[Edge],
    ) -> None:
        for user in self._paginate(
            client, f"/groups/{group_id}/users", {"limit": _PAGE_LIMIT},
        ):
            user_urn = user_urn_by_id.get(user["id"])
            if user_urn is None:
                continue
            edges.append(IdpPartOfEdge.create(
                organization_id=self.organization_id,
                from_urn=user_urn,
                to_urn=group_urn,
            ))

    def _load_app_user_assignments(
        self,
        client: httpx.Client,
        app_id: str,
        app_urn: URN,
        user_urn_by_id: dict[str, URN],
        edges: list[Edge],
    ) -> None:
        for app_user in self._paginate(
            client, f"/apps/{app_id}/users", {"limit": _PAGE_LIMIT},
        ):
            user_urn = user_urn_by_id.get(app_user.get("id", ""))
            if user_urn is None:
                continue
            edges.append(IdpAssignedToEdge.create(
                organization_id=self.organization_id,
                from_urn=user_urn,
                to_urn=app_urn,
            ))

    def _load_app_group_assignments(
        self,
        client: httpx.Client,
        app_id: str,
        app_urn: URN,
        group_urn_by_id: dict[str, URN],
        edges: list[Edge],
    ) -> None:
        for assignment in self._paginate(
            client, f"/apps/{app_id}/groups", {"limit": _PAGE_LIMIT},
        ):
            group_urn = group_urn_by_id.get(assignment.get("id", ""))
            if group_urn is None:
                continue
            edges.append(IdpAssignedToEdge.create(
                organization_id=self.organization_id,
                from_urn=group_urn,
                to_urn=app_urn,
            ))

    def _load_app_group_push(
        self,
        client: httpx.Client,
        app_id: str,
        app_urn: URN,
        group_urn_by_id: dict[str, URN],
        edges: list[Edge],
    ) -> None:
        try:
            mappings = list(self._paginate(
                client,
                f"/apps/{app_id}/group-push/mappings",
                {"limit": _PAGE_LIMIT},
            ))
        except httpx.HTTPStatusError as exc:
            # Group Push isn't enabled for every app type; non-200 here is expected.
            if exc.response.status_code in (400, 403, 404):
                return
            raise
        for mapping in mappings:
            source_group_id = mapping.get("sourceGroupId")
            if not source_group_id:
                continue
            source_urn = group_urn_by_id.get(source_group_id)
            if source_urn is None:
                continue
            edges.append(IdpPushesToEdge.create(
                organization_id=self.organization_id,
                from_urn=source_urn,
                to_urn=app_urn,
            ))

    # ── HTTP helpers ──────────────────────────────────────────────────────

    def _paginate(
        self,
        client: httpx.Client,
        path: str,
        params: dict[str, Any],
    ):
        """Yield items from a paginated Okta endpoint, following Link: rel="next" headers."""
        url: str | None = path
        next_params: dict[str, Any] | None = params
        while url is not None:
            response = client.get(url, params=next_params)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                return
            yield from payload
            url = _next_link(response.headers.get("link", ""))
            # The 'next' URL is absolute and already carries query params; pass None.
            next_params = None


def _full_name(profile: dict) -> str | None:
    first = profile.get("firstName") or ""
    last = profile.get("lastName") or ""
    full = f"{first} {last}".strip()
    return full or None


def _next_link(link_header: str) -> str | None:
    """Extract the rel=\"next\" URL from an Okta Link header, if any."""
    if not link_header:
        return None
    match = _LINK_NEXT_RE.search(link_header)
    return match.group(1) if match else None
