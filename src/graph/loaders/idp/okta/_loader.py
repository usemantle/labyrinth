"""OktaLoader — discovers Persons, Groups, Applications, and Okta-sourced edges."""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from typing import Any
from urllib.parse import parse_qs, urlparse

from okta.client import Client as OktaClient

from src.graph.credentials import CredentialBase, OktaTokenCredential
from src.graph.edges.okta_edges import (
    OktaAssignedToEdge,
    OktaPartOfEdge,
    OktaPushesToEdge,
)
from src.graph.graph_models import URN, Edge, Node
from src.graph.loaders.loader import ConceptLoader, URNComponent
from src.graph.nodes.okta import ApplicationNode, GroupNode, PersonNode

logger = logging.getLogger(__name__)

_PAGE_LIMIT = 200
_LINK_NEXT_RE = re.compile(r'<([^>]+)>\s*;\s*rel="next"')


class _OktaPaginationError(RuntimeError):
    """Raised when an Okta SDK call returns an error during pagination."""


class OktaLoader(ConceptLoader):
    """Loader for an Okta organization using the official ``okta-sdk-python`` client.

    Discovers users (PersonNode), groups (GroupNode), applications (ApplicationNode),
    and Okta-sourced edges (okta:part_of, okta:assigned_to, okta:pushes_to) by paging
    through the Okta Core API with an SSWS token.
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
        return cls(project_id, urn.account, credentials["api_token"]), str(urn)

    def load(self, resource: str) -> tuple[list[Node], list[Edge]]:
        return asyncio.run(self._load_async())

    async def _load_async(self) -> tuple[list[Node], list[Edge]]:
        client = OktaClient({
            "orgUrl": f"https://{self._domain}",
            "token": self._api_token,
            "authorizationMode": "SSWS",
        })

        # Phase 1: top-level entity discovery (users, groups, apps) — independent, run in parallel.
        (user_nodes, user_urns), (group_nodes, group_urns), (app_nodes, app_urns) = await asyncio.gather(
            self._load_users(client),
            self._load_groups(client),
            self._load_applications(client),
        )

        # Phase 2: per-group memberships and per-app assignments / pushes — fan out, all parallel.
        edge_tasks: list = [
            self._load_group_memberships(client, gid, gurn, user_urns)
            for gid, gurn in group_urns.items()
        ]
        for app_id, app_urn in app_urns.items():
            edge_tasks.extend([
                self._load_app_user_assignments(client, app_id, app_urn, user_urns),
                self._load_app_group_assignments(client, app_id, app_urn, group_urns),
                self._load_app_group_push(client, app_id, app_urn, group_urns),
            ])
        edge_lists = await asyncio.gather(*edge_tasks) if edge_tasks else []

        nodes: list[Node] = [*user_nodes, *group_nodes, *app_nodes]
        edges: list[Edge] = [edge for sublist in edge_lists for edge in sublist]
        return nodes, edges

    # ── User / Group / Application discovery ──────────────────────────────
    # Each helper returns its own (nodes, urns) — no shared mutable state, so safe under
    # asyncio.gather() (or threads, if the SDK ever moves to a threaded executor).

    async def _load_users(self, client: OktaClient) -> tuple[list[Node], dict[str, URN]]:
        nodes: list[Node] = []
        urns: dict[str, URN] = {}
        async for user in self._paginate(client.list_users):
            urn = self.build_urn("user", user.id)
            urns[user.id] = urn
            profile = getattr(user, "profile", None)
            nodes.append(PersonNode.create(
                organization_id=self.organization_id,
                urn=urn,
                okta_id=user.id,
                email=getattr(profile, "email", None) if profile else None,
                login=getattr(profile, "login", None) if profile else None,
                status=getattr(user, "status", None),
                display_name=_full_name(profile),
            ))
        logger.info("Discovered %d Okta users", len(urns))
        return nodes, urns

    async def _load_groups(self, client: OktaClient) -> tuple[list[Node], dict[str, URN]]:
        nodes: list[Node] = []
        urns: dict[str, URN] = {}
        async for group in self._paginate(client.list_groups):
            urn = self.build_urn("group", group.id)
            urns[group.id] = urn
            inner = _group_profile_inner(group)
            nodes.append(GroupNode.create(
                organization_id=self.organization_id,
                urn=urn,
                okta_id=group.id,
                name=getattr(inner, "name", None),
                description=getattr(inner, "description", None),
            ))
        logger.info("Discovered %d Okta groups", len(urns))
        return nodes, urns

    async def _load_applications(self, client: OktaClient) -> tuple[list[Node], dict[str, URN]]:
        nodes: list[Node] = []
        urns: dict[str, URN] = {}
        async for app in self._paginate(client.list_applications):
            urn = self.build_urn("app", app.id)
            urns[app.id] = urn
            nodes.append(ApplicationNode.create(
                organization_id=self.organization_id,
                urn=urn,
                okta_id=app.id,
                name=getattr(app, "name", None),
                label=getattr(app, "label", None),
                sign_on_mode=getattr(app, "sign_on_mode", None),
                status=getattr(app, "status", None),
            ))
        logger.info("Discovered %d Okta applications", len(urns))
        return nodes, urns

    # ── Edge discovery ────────────────────────────────────────────────────

    async def _load_group_memberships(
        self,
        client: OktaClient,
        group_id: str,
        group_urn: URN,
        user_urn_by_id: dict[str, URN],
    ) -> list[Edge]:
        edges: list[Edge] = []
        async for user in self._paginate(client.list_group_users, group_id):
            user_urn = user_urn_by_id.get(user.id)
            if user_urn is None:
                continue
            edges.append(OktaPartOfEdge.create(
                organization_id=self.organization_id,
                from_urn=user_urn,
                to_urn=group_urn,
            ))
        return edges

    async def _load_app_user_assignments(
        self,
        client: OktaClient,
        app_id: str,
        app_urn: URN,
        user_urn_by_id: dict[str, URN],
    ) -> list[Edge]:
        edges: list[Edge] = []
        async for app_user in self._paginate(client.list_application_users, app_id):
            user_urn = user_urn_by_id.get(getattr(app_user, "id", ""))
            if user_urn is None:
                continue
            edges.append(OktaAssignedToEdge.create(
                organization_id=self.organization_id,
                from_urn=user_urn,
                to_urn=app_urn,
            ))
        return edges

    async def _load_app_group_assignments(
        self,
        client: OktaClient,
        app_id: str,
        app_urn: URN,
        group_urn_by_id: dict[str, URN],
    ) -> list[Edge]:
        edges: list[Edge] = []
        async for assignment in self._paginate(
            client.list_application_group_assignments, app_id,
        ):
            group_urn = group_urn_by_id.get(getattr(assignment, "id", ""))
            if group_urn is None:
                continue
            edges.append(OktaAssignedToEdge.create(
                organization_id=self.organization_id,
                from_urn=group_urn,
                to_urn=app_urn,
            ))
        return edges

    async def _load_app_group_push(
        self,
        client: OktaClient,
        app_id: str,
        app_urn: URN,
        group_urn_by_id: dict[str, URN],
    ) -> list[Edge]:
        edges: list[Edge] = []
        try:
            async for mapping in self._paginate(client.list_group_push_mappings, app_id):
                source_id = getattr(mapping, "source_group_id", None)
                if not source_id:
                    continue
                source_urn = group_urn_by_id.get(source_id)
                if source_urn is None:
                    continue
                edges.append(OktaPushesToEdge.create(
                    organization_id=self.organization_id,
                    from_urn=source_urn,
                    to_urn=app_urn,
                ))
        except _OktaPaginationError as exc:
            # Group Push isn't enabled for every app type; non-success here is expected.
            logger.debug("Group Push not available for app %s: %s", app_id, exc)
        return edges

    # ── Pagination ────────────────────────────────────────────────────────

    async def _paginate(self, method, *args):
        """Yield items from an Okta SDK list_* method, following Link rel=\"next\" cursors."""
        after: str | None = None
        while True:
            kwargs: dict[str, Any] = {"limit": _PAGE_LIMIT}
            if after:
                kwargs["after"] = after
            items, resp, err = await method(*args, **kwargs)
            if err is not None:
                raise _OktaPaginationError(str(err))
            for item in items or []:
                yield item
            after = _next_cursor(getattr(resp, "headers", None))
            if after is None:
                return


def _full_name(profile) -> str | None:
    if profile is None:
        return None
    first = getattr(profile, "first_name", None) or ""
    last = getattr(profile, "last_name", None) or ""
    full = f"{first} {last}".strip()
    return full or None


def _group_profile_inner(group):
    """Return the concrete inner object from a Group's polymorphic profile field."""
    profile = getattr(group, "profile", None)
    if profile is None:
        return None
    return getattr(profile, "actual_instance", profile)


def _next_cursor(headers) -> str | None:
    """Extract the 'after' cursor from an Okta Link rel=\"next\" header."""
    if not headers:
        return None
    link = headers.get("link") or headers.get("Link")
    if not link:
        return None
    match = _LINK_NEXT_RE.search(link)
    if not match:
        return None
    parsed = urlparse(match.group(1))
    values = parse_qs(parsed.query).get("after")
    return values[0] if values else None
