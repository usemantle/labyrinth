"""
On-premises PostgreSQL loader for the security graph.

URN scheme: urn:onprem:postgres:{host}:{port}:{path}

The host and port identify the specific database instance. Path segments
follow the standard database/schema/table/column hierarchy.
"""

import uuid

from sqlalchemy.engine.url import make_url

from src.graph.graph_models import URN
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
