"""
AWS RDS / Aurora PostgreSQL loader for the security graph.

URN scheme: urn:aws:rds:{account_id}:{region}:{identifier}/{path}

The identifier is the RDS instance or Aurora cluster identifier parsed
from the provided ARN.  The database name, schema, table, and column
follow as path segments beneath it.
"""

import uuid

from src.graph.graph_models import Node, NodeMetadataKey, URN
from src.graph.loaders.postgres.postgres_loader import PostgresLoader


class RDSPostgresLoader(PostgresLoader):
    """Loader for AWS RDS and Aurora PostgreSQL instances."""

    def __init__(self, organization_id: uuid.UUID, arn: str):
        super().__init__(organization_id)
        self._arn = arn

        # arn:aws:rds:{region}:{account}:{resource_type}:{identifier}
        parts = arn.split(":")
        if len(parts) < 7 or parts[0] != "arn":
            raise ValueError(f"Invalid RDS ARN: {arn}")

        self._region = parts[3]
        self._account_id = parts[4]
        self._resource_type = parts[5]  # "db" or "cluster"
        self._identifier = parts[6]

    def build_urn(self, *path_segments: str) -> URN:
        path = "/".join(path_segments)
        return URN(
            f"urn:aws:rds:{self._account_id}:{self._region}:"
            f"{self._identifier}/{path}"
        )

    def _build_database_node(
        self,
        db_urn: URN,
        database_name: str,
        host: str,
        port: int,
    ) -> Node:
        node = super()._build_database_node(db_urn, database_name, host, port)
        node.metadata.update({
            NodeMetadataKey.ARN: self._arn,
            NodeMetadataKey.ACCOUNT_ID: self._account_id,
            NodeMetadataKey.REGION: self._region,
        })
        return node
