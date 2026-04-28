"""Unit tests for OktaLoader.

All HTTP calls are intercepted via httpx.MockTransport — no network access required.
"""

from __future__ import annotations

import json
import uuid

import httpx
import pytest

from src.graph.graph_models import URN, EdgeType, NodeMetadataKey, NodeType
from src.graph.loaders.idp.okta._loader import OktaLoader

NK = NodeMetadataKey
ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
DOMAIN = "yourorg.okta.com"
TOKEN = "ssws-token-test"


class FakeOkta:
    """Builds an httpx.MockTransport that serves canned responses for Okta endpoints.

    Each endpoint can return one or more pages. Pages 2..N are accessed via the
    `Link: <...>; rel="next"` response header that the loader follows.
    """

    def __init__(self) -> None:
        # Map (path, after_cursor or '') -> (json_body, next_cursor or None).
        self._pages: dict[tuple[str, str], tuple[list[dict], str | None]] = {}

    def add(self, path: str, pages: list[list[dict]]) -> None:
        for i, page in enumerate(pages):
            cursor_in = "" if i == 0 else f"page-{i}"
            cursor_out = f"page-{i + 1}" if i + 1 < len(pages) else None
            self._pages[(path, cursor_in)] = (page, cursor_out)

    def transport(self) -> httpx.MockTransport:
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.headers.get("Authorization") == f"SSWS {TOKEN}"
            path = request.url.path
            after = request.url.params.get("after", "")
            key = (path, after)
            if key not in self._pages:
                return httpx.Response(404, json={"error": "no fixture", "path": path, "after": after})
            body, next_cursor = self._pages[key]
            headers = {}
            if next_cursor is not None:
                next_url = f"https://{DOMAIN}{path}?limit=200&after={next_cursor}"
                headers["link"] = f'<{next_url}>; rel="next"'
            return httpx.Response(200, content=json.dumps(body), headers=headers)

        return httpx.MockTransport(handler)


@pytest.fixture
def patched_loader(monkeypatch):
    """Return a (loader, fake) pair where the loader's httpx.Client is bound to fake's transport."""
    fake = FakeOkta()

    real_client_init = httpx.Client.__init__

    def patched_init(self, *args, **kwargs):
        kwargs["transport"] = fake.transport()
        real_client_init(self, *args, **kwargs)

    monkeypatch.setattr(httpx.Client, "__init__", patched_init)
    loader = OktaLoader(ORG_ID, DOMAIN, TOKEN)
    return loader, fake


class TestOktaLoaderClassmethods:
    def test_display_name(self):
        assert OktaLoader.display_name() == "Okta"

    def test_urn_components(self):
        components = OktaLoader.urn_components()
        assert len(components) == 1
        assert components[0].name == "domain"

    def test_build_target_urn(self):
        urn = OktaLoader.build_target_urn(domain=DOMAIN)
        assert str(urn) == f"urn:okta:idp:{DOMAIN}::root"

    def test_from_target_config(self):
        urn = OktaLoader.build_target_urn(domain=DOMAIN)
        loader, resource = OktaLoader.from_target_config(
            ORG_ID, urn, {"api_token": TOKEN, "domain": DOMAIN},
        )
        assert isinstance(loader, OktaLoader)
        assert resource == str(urn)


class TestOktaLoaderLoad:
    def test_loads_users_groups_and_apps(self, patched_loader):
        loader, fake = patched_loader
        fake.add("/api/v1/users", [[
            {
                "id": "00u1",
                "status": "ACTIVE",
                "profile": {
                    "email": "alice@example.com",
                    "login": "alice@example.com",
                    "firstName": "Alice",
                    "lastName": "Anderson",
                },
            },
            {
                "id": "00u2",
                "status": "ACTIVE",
                "profile": {
                    "email": "bob@example.com",
                    "login": "bob@example.com",
                    "firstName": "Bob",
                    "lastName": "Brown",
                },
            },
        ]])
        fake.add("/api/v1/groups", [[
            {"id": "00g1", "profile": {"name": "Engineering", "description": "Eng team"}},
        ]])
        fake.add("/api/v1/apps", [[
            {
                "id": "0oa1",
                "name": "amazon_aws",
                "label": "AWS Account Federation",
                "signOnMode": "SAML_2_0",
                "status": "ACTIVE",
            },
        ]])
        fake.add("/api/v1/groups/00g1/users", [[
            {"id": "00u1"},
            {"id": "00u2"},
        ]])
        fake.add("/api/v1/apps/0oa1/users", [[
            {"id": "00u1"},
        ]])
        fake.add("/api/v1/apps/0oa1/groups", [[
            {"id": "00g1"},
        ]])
        fake.add("/api/v1/apps/0oa1/group-push/mappings", [[
            {"sourceGroupId": "00g1", "targetGroupId": "tg-1", "status": "ACTIVE"},
        ]])

        nodes, edges = loader.load(f"urn:okta:idp:{DOMAIN}::root")

        people = [n for n in nodes if n.node_type == NodeType.PERSON]
        groups = [n for n in nodes if n.node_type == NodeType.GROUP]
        apps = [n for n in nodes if n.node_type == NodeType.APPLICATION]
        assert len(people) == 2
        assert len(groups) == 1
        assert len(apps) == 1

        alice = next(n for n in people if n.metadata[NK.PERSON_OKTA_ID] == "00u1")
        assert str(alice.urn) == f"urn:okta:idp:{DOMAIN}::user/00u1"
        assert alice.metadata[NK.PERSON_EMAIL] == "alice@example.com"
        assert alice.metadata[NK.PERSON_DISPLAY_NAME] == "Alice Anderson"

        app = apps[0]
        assert app.metadata[NK.APP_SIGN_ON_MODE] == "SAML_2_0"

        part_of = [e for e in edges if e.edge_type == EdgeType.IDP_PART_OF]
        assigned_to = [e for e in edges if e.edge_type == EdgeType.IDP_ASSIGNED_TO]
        pushes_to = [e for e in edges if e.edge_type == EdgeType.IDP_PUSHES_TO]
        assert len(part_of) == 2
        assert len(assigned_to) == 2  # one user, one group
        assert len(pushes_to) == 1

        # Person -> Group
        assert any(
            str(e.from_urn) == f"urn:okta:idp:{DOMAIN}::user/00u1"
            and str(e.to_urn) == f"urn:okta:idp:{DOMAIN}::group/00g1"
            for e in part_of
        )
        # Group -> App push
        assert str(pushes_to[0].from_urn) == f"urn:okta:idp:{DOMAIN}::group/00g1"
        assert str(pushes_to[0].to_urn) == f"urn:okta:idp:{DOMAIN}::app/0oa1"

    def test_paginates_users(self, patched_loader):
        loader, fake = patched_loader
        # Two pages of users
        fake.add("/api/v1/users", [
            [{"id": "00u1", "status": "ACTIVE", "profile": {"email": "a@x.com"}}],
            [{"id": "00u2", "status": "ACTIVE", "profile": {"email": "b@x.com"}}],
        ])
        fake.add("/api/v1/groups", [[]])
        fake.add("/api/v1/apps", [[]])
        nodes, _ = loader.load(f"urn:okta:idp:{DOMAIN}::root")
        people = [n for n in nodes if n.node_type == NodeType.PERSON]
        assert {n.metadata[NK.PERSON_OKTA_ID] for n in people} == {"00u1", "00u2"}

    def test_group_push_404_is_silenced(self, patched_loader):
        loader, fake = patched_loader
        fake.add("/api/v1/users", [[]])
        fake.add("/api/v1/groups", [[]])
        fake.add("/api/v1/apps", [[
            {"id": "0oa1", "name": "x", "label": "x", "signOnMode": "SAML_2_0", "status": "ACTIVE"},
        ]])
        # /apps/0oa1/users and /apps/0oa1/groups respond empty
        fake.add("/api/v1/apps/0oa1/users", [[]])
        fake.add("/api/v1/apps/0oa1/groups", [[]])
        # group-push/mappings has no fixture -> 404, must NOT raise
        nodes, edges = loader.load(f"urn:okta:idp:{DOMAIN}::root")
        # No push edges, but loader still returns successfully
        assert all(e.edge_type != EdgeType.IDP_PUSHES_TO for e in edges)
        apps = [n for n in nodes if n.node_type == NodeType.APPLICATION]
        assert len(apps) == 1

    def test_unknown_member_id_skipped(self, patched_loader):
        loader, fake = patched_loader
        fake.add("/api/v1/users", [[
            {"id": "00u1", "status": "ACTIVE", "profile": {"email": "a@x.com"}},
        ]])
        fake.add("/api/v1/groups", [[
            {"id": "00g1", "profile": {"name": "G"}},
        ]])
        fake.add("/api/v1/apps", [[]])
        # Group lists a member that wasn't returned by /users (e.g. paginated mismatch)
        fake.add("/api/v1/groups/00g1/users", [[
            {"id": "00u1"},
            {"id": "00u-orphan"},
        ]])
        _, edges = loader.load(f"urn:okta:idp:{DOMAIN}::root")
        part_of = [e for e in edges if e.edge_type == EdgeType.IDP_PART_OF]
        # Only the known user produces an edge
        assert len(part_of) == 1
        assert str(part_of[0].from_urn) == f"urn:okta:idp:{DOMAIN}::user/00u1"


class TestBuildUrn:
    def test_build_urn(self):
        loader = OktaLoader(ORG_ID, DOMAIN, TOKEN)
        assert str(loader.build_urn("user", "00u1")) == f"urn:okta:idp:{DOMAIN}::user/00u1"
        assert str(loader.build_urn("app", "0oa1")) == f"urn:okta:idp:{DOMAIN}::app/0oa1"


class TestDomainNormalization:
    """OktaLoader strips protocol prefixes and trailing slashes defensively."""

    @pytest.mark.parametrize("input_domain", [
        DOMAIN,
        f"https://{DOMAIN}",
        f"http://{DOMAIN}",
        f"https://{DOMAIN}/",
        f"{DOMAIN}/",
    ])
    def test_domain_is_normalized(self, input_domain):
        loader = OktaLoader(ORG_ID, input_domain, TOKEN)
        assert str(loader.build_urn("user", "x")) == f"urn:okta:idp:{DOMAIN}::user/x"


class TestRootUrnIsAvailable:
    def test_root_urn_format(self):
        urn = URN(f"urn:okta:idp:{DOMAIN}::root")
        assert urn.provider == "okta"
        assert urn.service == "idp"
        assert urn.account == DOMAIN
        assert urn.path == "root"
