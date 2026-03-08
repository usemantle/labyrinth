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
from src.graph.edges.contains_edge import ContainsEdge
from src.graph.edges.reads_edge import ReadsEdge
from src.graph.edges.references_edge import ReferencesEdge
from src.graph.edges.writes_edge import WritesEdge
from src.graph.graph_models import (
    URN,
    Edge,
    EdgeMetadata,
    EdgeMetadataKey,
    Node,
)
from src.graph.loaders.loader import ConceptLoader
from src.graph.nodes.column_node import ColumnNode
from src.graph.nodes.database_node import DatabaseNode
from src.graph.nodes.identity_node import IdentityNode
from src.graph.nodes.schema_node import SchemaNode
from src.graph.nodes.table_node import TableNode

logger = logging.getLogger(__name__)

# Privileges that map to WritesEdge; everything else defaults to ReadsEdge.
_WRITE_PRIVILEGES = frozenset({"INSERT", "UPDATE", "DELETE"})


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

        # Foreign-key edges (column → column, ReferencesEdge)
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
    ) -> DatabaseNode:
        """Build the database root node.

        Override in subclasses to add provider-specific metadata
        (e.g. ARN, account_id, region for AWS).
        """
        return DatabaseNode.create(
            self.organization_id,
            db_urn,
            parent_urn=None,
            database_name=database_name,
            host=host,
            port=port,
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

            nodes.append(SchemaNode.create(
                self.organization_id,
                schema_urn,
                parent_urn=db_urn,
                schema_name=schema_name,
            ))
            edges.append(ContainsEdge.create(
                self.organization_id, db_urn, schema_urn,
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

            nodes.append(TableNode.create(
                self.organization_id,
                table_urn,
                parent_urn=schema_urn,
                table_name=table_name,
                table_type=table.table_type,
            ))
            edges.append(ContainsEdge.create(
                self.organization_id, schema_urn, table_urn,
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

            nodes.append(ColumnNode.create(
                self.organization_id,
                col_urn,
                parent_urn=table_urn,
                column_name=col.column_name,
                data_type=_format_data_type(col),
                nullable=col.is_nullable,
                ordinal_position=ordinal,
            ))
            edges.append(ContainsEdge.create(
                self.organization_id, table_urn, col_urn,
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
            nodes.append(IdentityNode.create(
                self.organization_id,
                role_urn,
                parent_urn=None,
                role_name=role.role_name,
                role_login=role.can_login,
                role_superuser=role.is_superuser,
            ))

        # Discover grants and create ReadsEdge / WritesEdge edges
        for grant in driver.discover_grants():
            role_urn = role_urn_map.get(grant.grantee)
            if not role_urn:
                continue

            table_urn = self.build_urn(
                database_name, grant.table_schema, grant.table_name,
            )

            privilege = grant.privilege_type
            grant_metadata = EdgeMetadata({
                EdgeMetadataKey.PRIVILEGE: privilege,
            })

            if privilege == "ALL":
                edges.append(ReadsEdge.create(
                    self.organization_id,
                    role_urn,
                    table_urn,
                    metadata=grant_metadata,
                ))
                edges.append(WritesEdge.create(
                    self.organization_id,
                    role_urn,
                    table_urn,
                    metadata=grant_metadata,
                ))
            elif privilege in _WRITE_PRIVILEGES:
                edges.append(WritesEdge.create(
                    self.organization_id,
                    role_urn,
                    table_urn,
                    metadata=grant_metadata,
                ))
            else:
                edges.append(ReadsEdge.create(
                    self.organization_id,
                    role_urn,
                    table_urn,
                    metadata=grant_metadata,
                ))

        return nodes, edges

    def _discover_foreign_keys(
        self,
        driver: BaseDiscoveryDriver,
        database_name: str,
    ) -> list[Edge]:
        """Discover foreign-key relationships as ReferencesEdge edges."""
        edges: list[Edge] = []

        for fk in driver.discover_foreign_keys():
            from_urn = self.build_urn(
                database_name, fk.fk_schema, fk.fk_table, fk.fk_column,
            )
            to_urn = self.build_urn(
                database_name, fk.ref_schema, fk.ref_table, fk.ref_column,
            )
            edges.append(ReferencesEdge.create(
                self.organization_id,
                from_urn,
                to_urn,
                metadata=EdgeMetadata({
                    EdgeMetadataKey.CONSTRAINT_NAME: fk.constraint_name,
                    EdgeMetadataKey.ORDINAL_POSITION: fk.ordinal_position,
                }),
            ))

        return edges
