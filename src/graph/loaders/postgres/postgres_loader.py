"""
Abstract PostgreSQL loader for the security graph.

Uses PostgresDiscoveryDriver to discover schemas, tables, columns, and
foreign keys, and transforms them into graph Nodes and Edges.

Concrete subclasses (OnPremPostgresLoader, etc.)
provide URN construction via build_urn().
"""

import abc
import logging

from sqlalchemy.engine.url import make_url

from src.drivers.sql.base import BaseDiscoveryDriver
from src.drivers.sql.models import ColumnMetadata
from src.graph.graph_models import (
    Edge,
    EdgeMetadata,
    EdgeMetadataKey,
    Node,
    NodeMetadata,
    NodeMetadataKey,
    RelationType,
    URN,
)
from src.graph.loaders._helpers import make_edge
from src.graph.loaders.loader import ConceptLoader

logger = logging.getLogger(__name__)


def _format_data_type(col: ColumnMetadata) -> str:
    """Build a full data type string from column metadata.

    Mirrors the logic in db_discovery_job.py:210-218.
    """
    if col.character_maximum_length:
        return f"{col.data_type}({col.character_maximum_length})"
    elif col.numeric_precision and col.numeric_scale:
        return f"{col.data_type}({col.numeric_precision},{col.numeric_scale})"
    elif col.numeric_precision:
        return f"{col.data_type}({col.numeric_precision})"
    return col.data_type


class PostgresLoader(ConceptLoader, abc.ABC):
    """Abstract PostgreSQL loader.

    Implements ``load()`` using ``PostgresDiscoveryDriver`` to discover
    database metadata and produce graph Nodes and Edges.  The
    ``build_urn()`` method remains abstract so concrete subclasses can
    define provider-specific URN schemes (e.g. AWS RDS vs on-prem).
    """

    def load(self, resource: str) -> tuple[list[Node], list[Edge]]:
        """Discover nodes and edges from a PostgreSQL connection string.

        Args:
            resource: A ``postgresql://`` connection string.

        Returns:
            A tuple of (nodes, edges) discovered from the database.
        """
        url = make_url(resource)
        host = url.host or "localhost"
        port = url.port or 5432
        database_name = url.database or ""

        driver = BaseDiscoveryDriver.get_driver("POSTGRES", resource)

        nodes: list[Node] = []
        edges: list[Edge] = []

        # Database node (root — no parent, no CONTAINS edge)
        db_urn = self.build_urn(database_name)
        nodes.append(self._build_database_node(db_urn, database_name, host, port))

        # Structural discovery: schemas → tables → columns
        schema_nodes, schema_edges = self._discover_schemas_in_db(
            driver, database_name, db_urn,
        )
        nodes.extend(schema_nodes)
        edges.extend(schema_edges)

        # Foreign-key edges (column → column, DATA_TO_DATA)
        fk_edges = self._discover_foreign_keys(driver, database_name)
        edges.extend(fk_edges)

        # Role & grant discovery
        role_nodes, grant_edges = self._discover_roles_and_grants(
            driver, database_name,
        )
        nodes.extend(role_nodes)
        edges.extend(grant_edges)

        logger.info(
            "Discovered %d nodes and %d edges from %s",
            len(nodes), len(edges), database_name,
        )

        return nodes, edges

    def _build_database_node(
        self,
        db_urn: URN,
        database_name: str,
        host: str,
        port: int,
    ) -> Node:
        """Build the database root node.

        Override in subclasses to add provider-specific metadata
        (e.g. ARN, account_id, region for AWS).
        """
        return Node(
            organization_id=self.organization_id,
            urn=db_urn,
            parent_urn=None,
            metadata=NodeMetadata({
                NodeMetadataKey.DATABASE_NAME: database_name,
                NodeMetadataKey.HOST: host,
                NodeMetadataKey.PORT: port,
            }),
        )

    # ------------------------------------------------------------------
    # Private discovery helpers
    # ------------------------------------------------------------------

    def _discover_schemas_in_db(
        self,
        driver: BaseDiscoveryDriver,
        database_name: str,
        db_urn: URN,
    ) -> tuple[list[Node], list[Edge]]:
        """Discover schemas and everything beneath them.

        Returns schema/table/column nodes and all CONTAINS edges from
        db→schema, schema→table, and table→column.
        """
        nodes: list[Node] = []
        edges: list[Edge] = []

        for schema in driver.discover_schemas(database_name):
            schema_name = schema.schema_name
            schema_urn = self.build_urn(database_name, schema_name)

            nodes.append(Node(
                organization_id=self.organization_id,
                urn=schema_urn,
                parent_urn=db_urn,
                metadata=NodeMetadata({NodeMetadataKey.SCHEMA_NAME: schema_name}),
            ))
            edges.append(make_edge(
                self.organization_id, db_urn, schema_urn, RelationType.CONTAINS,
            ))

            table_nodes, table_edges = self._discover_tables_in_schema(
                driver, database_name, schema_name, schema_urn,
            )
            nodes.extend(table_nodes)
            edges.extend(table_edges)

        return nodes, edges

    def _discover_tables_in_schema(
        self,
        driver: BaseDiscoveryDriver,
        database_name: str,
        schema_name: str,
        schema_urn: URN,
    ) -> tuple[list[Node], list[Edge]]:
        """Discover tables and their columns within a schema."""
        nodes: list[Node] = []
        edges: list[Edge] = []

        for table in driver.discover_tables(schema_name):
            table_name = table.table_name
            table_urn = self.build_urn(database_name, schema_name, table_name)

            nodes.append(Node(
                organization_id=self.organization_id,
                urn=table_urn,
                parent_urn=schema_urn,
                metadata=NodeMetadata({
                    NodeMetadataKey.TABLE_NAME: table_name,
                    NodeMetadataKey.TABLE_TYPE: table.table_type,
                }),
            ))
            edges.append(make_edge(
                self.organization_id, schema_urn, table_urn, RelationType.CONTAINS,
            ))

            col_nodes, col_edges = self._discover_columns_in_table(
                driver, database_name, schema_name, table_name, table_urn,
            )
            nodes.extend(col_nodes)
            edges.extend(col_edges)

        return nodes, edges

    def _discover_columns_in_table(
        self,
        driver: BaseDiscoveryDriver,
        database_name: str,
        schema_name: str,
        table_name: str,
        table_urn: URN,
    ) -> tuple[list[Node], list[Edge]]:
        """Discover columns within a table."""
        nodes: list[Node] = []
        edges: list[Edge] = []

        for ordinal, col in enumerate(driver.discover_columns(schema_name, table_name)):
            col_urn = self.build_urn(
                database_name, schema_name, table_name, col.column_name,
            )

            nodes.append(Node(
                organization_id=self.organization_id,
                urn=col_urn,
                parent_urn=table_urn,
                metadata=NodeMetadata({
                    NodeMetadataKey.COLUMN_NAME: col.column_name,
                    NodeMetadataKey.DATA_TYPE: _format_data_type(col),
                    NodeMetadataKey.NULLABLE: col.is_nullable,
                    NodeMetadataKey.ORDINAL_POSITION: ordinal,
                }),
            ))
            edges.append(make_edge(
                self.organization_id, table_urn, col_urn, RelationType.CONTAINS,
            ))

        return nodes, edges

    def _discover_roles_and_grants(
        self,
        driver: BaseDiscoveryDriver,
        database_name: str,
    ) -> tuple[list[Node], list[Edge]]:
        """Discover database roles and their table grants."""
        nodes: list[Node] = []
        edges: list[Edge] = []

        # Discover roles
        role_urn_map: dict[str, URN] = {}
        for role in driver.discover_roles(database_name):
            role_urn = self.build_urn(database_name, "roles", role.role_name)
            role_urn_map[role.role_name] = role_urn
            nodes.append(Node(
                organization_id=self.organization_id,
                urn=role_urn,
                parent_urn=None,
                metadata=NodeMetadata({
                    NodeMetadataKey.ROLE_NAME: role.role_name,
                    NodeMetadataKey.ROLE_LOGIN: role.can_login,
                    NodeMetadataKey.ROLE_SUPERUSER: role.is_superuser,
                }),
            ))

        # Discover grants and create PRINCIPAL_TO_DATA edges
        for grant in driver.discover_grants():
            role_urn = role_urn_map.get(grant.grantee)
            if not role_urn:
                continue

            table_urn = self.build_urn(
                database_name, grant.table_schema, grant.table_name,
            )

            edges.append(make_edge(
                self.organization_id,
                role_urn,
                table_urn,
                RelationType.PRINCIPAL_TO_DATA,
                metadata=EdgeMetadata({
                    EdgeMetadataKey.PRIVILEGE: grant.privilege_type,
                }),
            ))

        return nodes, edges

    def _discover_foreign_keys(
        self,
        driver: BaseDiscoveryDriver,
        database_name: str,
    ) -> list[Edge]:
        """Discover foreign-key relationships as DATA_TO_DATA edges."""
        edges: list[Edge] = []

        for fk in driver.discover_foreign_keys():
            from_urn = self.build_urn(
                database_name, fk.fk_schema, fk.fk_table, fk.fk_column,
            )
            to_urn = self.build_urn(
                database_name, fk.ref_schema, fk.ref_table, fk.ref_column,
            )
            edges.append(make_edge(
                self.organization_id,
                from_urn,
                to_urn,
                RelationType.DATA_TO_DATA,
                metadata=EdgeMetadata({
                    EdgeMetadataKey.CONSTRAINT_NAME: fk.constraint_name,
                    EdgeMetadataKey.ORDINAL_POSITION: fk.ordinal_position,
                }),
            ))

        return edges
