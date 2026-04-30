"""Unit tests for OktaLoader.

Mocks the Okta SDK Client directly. No network or asyncio plumbing in the test
body — OktaLoader.load() runs the async work itself via asyncio.run(), so each
test simply seeds the fake client with canned responses and calls load().
"""

from __future__ import annotations

import json
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.graph.graph_models import URN, EdgeType, NodeMetadataKey, NodeType
from src.graph.loaders.idp.okta._loader import OktaLoader

NK = NodeMetadataKey
ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
DOMAIN = "yourorg.okta.com"
TOKEN = "ssws-token-test"


# ── Fake response helpers ─────────────────────────────────────────────────


def _resp(headers: dict | None = None) -> MagicMock:
    """Build a fake ApiResponse with the given headers (used for Link cursors)."""
    return MagicMock(headers=headers or {})


def _link_next(after: str) -> dict:
    return {"link": f'<https://{DOMAIN}/api/v1/users?limit=200&after={after}>; rel="next"'}


def _user(uid, *, email=None, login=None, status="ACTIVE", first=None, last=None):
    return SimpleNamespace(
        id=uid, status=status,
        profile=SimpleNamespace(email=email, login=login, first_name=first, last_name=last),
    )


def _group(gid, *, name=None, description=None):
    inner = SimpleNamespace(name=name, description=description)
    return SimpleNamespace(id=gid, profile=SimpleNamespace(actual_instance=inner))


def _app(aid, *, name=None, label=None, sign_on_mode="SAML_2_0", status="ACTIVE"):
    """Build a raw-JSON app dict matching what the Okta REST API returns."""
    return {
        "id": aid,
        "name": name,
        "label": label,
        "signOnMode": sign_on_mode,
        "status": status,
    }


class _PagedRawAppsExecutor:
    """Fake RequestExecutor that serves preconfigured app pages over the SDK contract.

    Pages are a list of ``(items, link_header)`` tuples. Each ``execute`` call pops
    the next page; ``link_header`` is None if no further pages exist.
    """

    def __init__(self, pages: list[tuple[list[dict], str | None]] | None = None):
        self._pages = list(pages or [])

    async def create_request(self, method: str, url: str, **kwargs):
        return ({"method": method, "url": url}, None)

    async def execute(self, request, response_type=None):
        if not self._pages:
            return (MagicMock(headers={}), "[]", None)
        items, link = self._pages.pop(0)
        headers = {"link": link} if link else {}
        response = MagicMock(headers=headers)
        return (response, json.dumps(items), None)


def _app_user(uid):
    return SimpleNamespace(id=uid)


def _app_group(gid):
    return SimpleNamespace(id=gid)


def _push_mapping(source_group_id, *, target_group_id="tg", status="ACTIVE"):
    return SimpleNamespace(
        source_group_id=source_group_id, target_group_id=target_group_id, status=status,
    )


# ── Fixture: patch OktaClient + return a configurable fake ────────────────


@pytest.fixture
def fake_client(monkeypatch):
    """Replaces OktaClient with a MagicMock whose list_* methods return mockable async results.

    The raw application discovery (``client.get_request_executor()``) is also mocked so
    tests can configure paginated app responses by setting ``client._raw_app_pages``.
    """
    client = MagicMock()

    # Default: every SDK list_* returns a single empty page.
    for method in [
        "list_users", "list_groups",
        "list_group_users", "list_application_users",
        "list_application_group_assignments", "list_group_push_mappings",
    ]:
        setattr(client, method, AsyncMock(return_value=([], _resp(), None)))

    # Default raw app pages: one empty page (no apps).
    client._raw_app_pages = [([], None)]

    def get_request_executor():
        # Re-build the executor each time so each call sees a fresh copy of the
        # caller-configured pages. Tests assign ``client._raw_app_pages`` BEFORE calling load().
        return _PagedRawAppsExecutor(list(client._raw_app_pages))

    client.get_request_executor = get_request_executor

    monkeypatch.setattr(
        "src.graph.loaders.idp.okta._loader.OktaClient",
        lambda config: client,
    )
    return client


# ── Classmethod / construction tests ──────────────────────────────────────


class TestOktaLoaderClassmethods:
    def test_display_name(self):
        assert OktaLoader.display_name() == "Okta"

    def test_urn_components(self):
        components = OktaLoader.urn_components()
        assert len(components) == 1
        assert components[0].name == "domain"

    def test_credential_type(self):
        from src.graph.credentials import OktaTokenCredential
        assert OktaLoader.credential_type() is OktaTokenCredential

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


class TestBuildUrn:
    def test_build_urn(self):
        loader = OktaLoader(ORG_ID, DOMAIN, TOKEN)
        assert str(loader.build_urn("user", "00u1")) == f"urn:okta:idp:{DOMAIN}::user/00u1"
        assert str(loader.build_urn("app", "0oa1")) == f"urn:okta:idp:{DOMAIN}::app/0oa1"


class TestDomainNormalization:
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


# ── End-to-end load() tests ───────────────────────────────────────────────


class TestOktaLoaderLoad:
    def test_loads_users_groups_and_apps(self, fake_client):
        fake_client.list_users.return_value = (
            [
                _user("00u1", email="alice@example.com", login="alice@example.com",
                      first="Alice", last="Anderson"),
                _user("00u2", email="bob@example.com", login="bob@example.com",
                      first="Bob", last="Brown"),
            ],
            _resp(), None,
        )
        fake_client.list_groups.return_value = (
            [_group("00g1", name="Engineering", description="Eng team")], _resp(), None,
        )
        fake_client._raw_app_pages = [
            ([_app("0oa1", name="amazon_aws", label="AWS Account Federation")], None),
        ]
        fake_client.list_group_users.return_value = (
            [_app_user("00u1"), _app_user("00u2")], _resp(), None,
        )
        fake_client.list_application_users.return_value = (
            [_app_user("00u1")], _resp(), None,
        )
        fake_client.list_application_group_assignments.return_value = (
            [_app_group("00g1")], _resp(), None,
        )
        fake_client.list_group_push_mappings.return_value = (
            [_push_mapping("00g1")], _resp(), None,
        )

        loader = OktaLoader(ORG_ID, DOMAIN, TOKEN)
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
        assert app.metadata[NK.APP_LABEL] == "AWS Account Federation"

        part_of = [e for e in edges if e.edge_type == EdgeType.OKTA_PART_OF]
        assigned_to = [e for e in edges if e.edge_type == EdgeType.OKTA_ASSIGNED_TO]
        pushes_to = [e for e in edges if e.edge_type == EdgeType.OKTA_PUSHES_TO]
        assert len(part_of) == 2
        assert len(assigned_to) == 2  # one user, one group
        assert len(pushes_to) == 1

        assert any(
            str(e.from_urn) == f"urn:okta:idp:{DOMAIN}::user/00u1"
            and str(e.to_urn) == f"urn:okta:idp:{DOMAIN}::group/00g1"
            for e in part_of
        )
        assert str(pushes_to[0].from_urn) == f"urn:okta:idp:{DOMAIN}::group/00g1"
        assert str(pushes_to[0].to_urn) == f"urn:okta:idp:{DOMAIN}::app/0oa1"

    def test_paginates_users(self, fake_client):
        # First call returns page 1 with a Link rel="next" header pointing at cursor "p2".
        # Second call (with after=p2) returns page 2 and no Link header.
        fake_client.list_users.side_effect = [
            ([_user("00u1", email="a@x.com")], _resp(_link_next("p2")), None),
            ([_user("00u2", email="b@x.com")], _resp(), None),
        ]
        loader = OktaLoader(ORG_ID, DOMAIN, TOKEN)
        nodes, _ = loader.load(f"urn:okta:idp:{DOMAIN}::root")
        people = [n for n in nodes if n.node_type == NodeType.PERSON]
        assert {n.metadata[NK.PERSON_OKTA_ID] for n in people} == {"00u1", "00u2"}

        # Second call must have been made with after="p2".
        assert fake_client.list_users.await_count == 2
        second_call_kwargs = fake_client.list_users.await_args_list[1].kwargs
        assert second_call_kwargs["after"] == "p2"

    def test_group_push_error_is_silenced(self, fake_client):
        # One application; group-push call returns an error (not enabled for app).
        fake_client._raw_app_pages = [
            ([_app("0oa1", label="x")], None),
        ]
        fake_client.list_group_push_mappings.return_value = (
            None, None, "Group Push not enabled for application",
        )

        loader = OktaLoader(ORG_ID, DOMAIN, TOKEN)
        nodes, edges = loader.load(f"urn:okta:idp:{DOMAIN}::root")

        # No push edges, but loader still returns successfully and discovers the app.
        assert all(e.edge_type != EdgeType.OKTA_PUSHES_TO for e in edges)
        apps = [n for n in nodes if n.node_type == NodeType.APPLICATION]
        assert len(apps) == 1

    def test_unknown_member_id_skipped(self, fake_client):
        # Group lists a member whose id wasn't returned by /users — must not crash, must skip.
        fake_client.list_users.return_value = (
            [_user("00u1", email="a@x.com")], _resp(), None,
        )
        fake_client.list_groups.return_value = (
            [_group("00g1", name="G")], _resp(), None,
        )
        fake_client.list_group_users.return_value = (
            [_app_user("00u1"), _app_user("00u-orphan")], _resp(), None,
        )

        loader = OktaLoader(ORG_ID, DOMAIN, TOKEN)
        _, edges = loader.load(f"urn:okta:idp:{DOMAIN}::root")

        part_of = [e for e in edges if e.edge_type == EdgeType.OKTA_PART_OF]
        assert len(part_of) == 1
        assert str(part_of[0].from_urn) == f"urn:okta:idp:{DOMAIN}::user/00u1"

    def test_non_push_error_propagates(self, fake_client):
        # A list_users error should NOT be silenced — it's not the group-push escape hatch.
        fake_client.list_users.return_value = (None, None, "boom")
        loader = OktaLoader(ORG_ID, DOMAIN, TOKEN)
        with pytest.raises(RuntimeError, match="boom"):
            loader.load(f"urn:okta:idp:{DOMAIN}::root")

    def test_apps_paginate_via_link_header(self, fake_client):
        # Two pages, second page has no Link header (last page).
        page1_link = (
            f'<https://{DOMAIN}/api/v1/apps?limit=200&after=cursor2>; rel="next"'
        )
        fake_client._raw_app_pages = [
            ([_app("0oa1", label="App One")], page1_link),
            ([_app("0oa2", label="App Two")], None),
        ]
        loader = OktaLoader(ORG_ID, DOMAIN, TOKEN)
        nodes, _ = loader.load(f"urn:okta:idp:{DOMAIN}::root")

        apps = [n for n in nodes if n.node_type == NodeType.APPLICATION]
        labels = {n.metadata[NK.APP_LABEL] for n in apps}
        assert labels == {"App One", "App Two"}

    def test_apps_with_missing_optional_fields_still_loaded(self, fake_client):
        # A SAML application missing every settings.signOn.* field — the SDK would
        # raise pydantic.ValidationError on this, but the raw path doesn't care.
        fake_client._raw_app_pages = [
            ([{
                "id": "0oaSAML",
                "name": "amazon_aws",
                "label": "Broken SAML App",
                "signOnMode": "SAML_2_0",
                "status": "ACTIVE",
                "settings": {"signOn": {"defaultRelayState": None}},
            }], None),
        ]
        loader = OktaLoader(ORG_ID, DOMAIN, TOKEN)
        nodes, _ = loader.load(f"urn:okta:idp:{DOMAIN}::root")

        apps = [n for n in nodes if n.node_type == NodeType.APPLICATION]
        assert len(apps) == 1
        assert apps[0].metadata[NK.APP_LABEL] == "Broken SAML App"
        assert apps[0].metadata[NK.APP_SIGN_ON_MODE] == "SAML_2_0"

    def test_apps_with_missing_id_skipped(self, fake_client):
        fake_client._raw_app_pages = [
            ([{"label": "Anonymous"}, {"id": "0oa-good", "label": "Has Id"}], None),
        ]
        loader = OktaLoader(ORG_ID, DOMAIN, TOKEN)
        nodes, _ = loader.load(f"urn:okta:idp:{DOMAIN}::root")
        apps = [n for n in nodes if n.node_type == NodeType.APPLICATION]
        assert {n.metadata[NK.APP_LABEL] for n in apps} == {"Has Id"}
