"""
On-premises PostgreSQL loader for the security graph.

URN scheme: urn:onprem:postgres:{host}:{port}:{path}

The host and port identify the specific database instance. Path segments
follow the standard database/schema/table/column hierarchy.
"""

from __future__ import annotations

import uuid

from sqlalchemy.engine.url import make_url

from src.graph.credentials import CredentialBase, UsernamePasswordCredential
from src.graph.graph_models import URN
from src.graph.loaders.loader import URNComponent
from src.graph.loaders.postgres.postgres_loader import PostgresLoader


class OnPremPostgresLoader(PostgresLoader):
    """Loader for self-hosted / on-premises PostgreSQL instances."""

    def __init__(self, organization_id: uuid.UUID, resource: str):
        super().__init__(organization_id)
        url = make_url(resource)
        self._host = url.host or "localhost"
        self._port = str(url.port or 5432)

    def build_urn(self, *path_segments: str) -> URN:
        path = "/".join(path_segments)
        return URN(f"urn:onprem:postgres:{self._host}:{self._port}:{path}")

    @classmethod
    def display_name(cls) -> str:
        return "PostgreSQL"

    @classmethod
    def urn_components(cls) -> list[URNComponent]:
        return [
            URNComponent("host", "Database hostname", default="localhost"),
            URNComponent("port", "Database port", default="5432"),
            URNComponent("database", "Database name"),
        ]

    @classmethod
    def credential_type(cls) -> type[CredentialBase]:
        return UsernamePasswordCredential

    @classmethod
    def build_target_urn(cls, **components: str) -> URN:
        return URN(f"urn:onprem:postgres:{components['host']}:{components['port']}:{components['database']}")
