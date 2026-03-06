"""
PostgreSQL discovery driver.

Implements database discovery for PostgreSQL using information_schema
and pg_indexes system tables.
"""

import re
import logging
from typing import ClassVar, List

from sqlalchemy import text

from src.drivers.sql.base import BaseDiscoveryDriver
from src.drivers.sql.models import (
    SchemaMetadata,
    TableMetadata,
    ColumnMetadata,
    ConstraintMetadata,
    ForeignKeyMetadata,
    GrantMetadata,
    IndexMetadata,
    RoleMetadata,
)

logger = logging.getLogger(__name__)


class PostgresDiscoveryDriver(BaseDiscoveryDriver):
    """PostgreSQL-specific discovery driver."""
    
    SYSTEM_SCHEMAS: ClassVar[List[str]] = [
        "pg_catalog",
        "information_schema", 
        "pg_toast",
    ]
    
    def discover_schemas(self, database_name: str) -> List[SchemaMetadata]:
        """Discover all schemas in the PostgreSQL database."""
        with self.engine.connect() as conn:
            exclusion_list = self.get_system_schemas_sql_list()
            query = text(f"""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ({exclusion_list})
                ORDER BY schema_name
            """)
            result = conn.execute(query).fetchall()
            
            return [
                SchemaMetadata(
                    schema_name=row[0],
                    database_name=database_name,
                )
                for row in result
            ]
    
    def discover_tables(self, schema_name: str) -> List[TableMetadata]:
        """Discover all tables in a PostgreSQL schema."""
        with self.engine.connect() as conn:
            query = text("""
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = :schema_name
                  AND table_type IN ('BASE TABLE', 'VIEW')
                ORDER BY table_name
            """)
            result = conn.execute(query, {"schema_name": schema_name}).fetchall()
            
            return [
                TableMetadata(
                    table_name=row[0],
                    table_type=row[1].upper().replace(" ", "_"),
                )
                for row in result
            ]
    
    def discover_columns(
        self,
        schema_name: str,
        table_name: str,
    ) -> List[ColumnMetadata]:
        """Discover all columns in a PostgreSQL table."""
        with self.engine.connect() as conn:
            query = text("""
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
                FROM information_schema.columns
                WHERE table_schema = :schema_name
                  AND table_name = :table_name
                ORDER BY ordinal_position
            """)
            result = conn.execute(
                query,
                {"schema_name": schema_name, "table_name": table_name}
            ).fetchall()
            
            return [
                ColumnMetadata(
                    column_name=row[0],
                    data_type=row[1],
                    is_nullable=row[2] == "YES",
                    character_maximum_length=row[3],
                    numeric_precision=row[4],
                    numeric_scale=row[5],
                )
                for row in result
            ]
    
    def discover_constraints(self) -> List[ConstraintMetadata]:
        """Discover PRIMARY KEY, UNIQUE, CHECK constraints."""
        with self.engine.connect() as conn:
            exclusion_list = self.get_system_schemas_sql_list()
            query = text(f"""
                SELECT
                    tc.constraint_name,
                    tc.constraint_type,
                    tc.table_schema,
                    tc.table_name,
                    kcu.column_name
                FROM information_schema.table_constraints tc
                LEFT JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                  AND tc.table_name = kcu.table_name
                WHERE tc.table_schema NOT IN ({exclusion_list})
                  AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'CHECK')
                ORDER BY tc.constraint_name, kcu.ordinal_position
            """)
            result = conn.execute(query).fetchall()
            
            constraints = []
            for row in result:
                constraint_name, constraint_type, schema_name, table_name, column_name = row
                
                # Map constraint type
                if constraint_type == "PRIMARY KEY":
                    ctype = "PRIMARY_KEY"
                elif constraint_type == "UNIQUE":
                    ctype = "UNIQUE"
                elif constraint_type == "CHECK":
                    ctype = "CHECK"
                else:
                    continue
                
                constraints.append(ConstraintMetadata(
                    constraint_name=constraint_name,
                    constraint_type=ctype,
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=column_name or "",
                ))
            
            return constraints
    
    def discover_foreign_keys(self) -> List[ForeignKeyMetadata]:
        """Discover foreign key relationships."""
        with self.engine.connect() as conn:
            exclusion_list = self.get_system_schemas_sql_list()
            query = text(f"""
                SELECT
                    rc.constraint_name,
                    kcu1.table_schema AS fk_schema,
                    kcu1.table_name AS fk_table,
                    kcu1.column_name AS fk_column,
                    kcu2.table_schema AS ref_schema,
                    kcu2.table_name AS ref_table,
                    kcu2.column_name AS ref_column,
                    kcu1.ordinal_position
                FROM information_schema.referential_constraints rc
                JOIN information_schema.key_column_usage kcu1
                  ON rc.constraint_name = kcu1.constraint_name
                  AND rc.constraint_schema = kcu1.constraint_schema
                JOIN information_schema.key_column_usage kcu2
                  ON rc.unique_constraint_name = kcu2.constraint_name
                  AND rc.unique_constraint_schema = kcu2.constraint_schema
                  AND kcu1.ordinal_position = kcu2.ordinal_position
                WHERE kcu1.table_schema NOT IN ({exclusion_list})
                ORDER BY rc.constraint_name, kcu1.ordinal_position
            """)
            result = conn.execute(query).fetchall()
            
            return [
                ForeignKeyMetadata(
                    constraint_name=row[0],
                    fk_schema=row[1],
                    fk_table=row[2],
                    fk_column=row[3],
                    ref_schema=row[4],
                    ref_table=row[5],
                    ref_column=row[6],
                    ordinal_position=row[7],
                )
                for row in result
            ]
    
    def discover_indexes(self) -> List[IndexMetadata]:
        """Discover indexes using pg_indexes."""
        with self.engine.connect() as conn:
            exclusion_list = self.get_system_schemas_sql_list()
            query = text(f"""
                SELECT
                    schemaname,
                    tablename,
                    indexname,
                    indexdef
                FROM pg_indexes
                WHERE schemaname NOT IN ({exclusion_list})
                ORDER BY schemaname, tablename, indexname
            """)
            result = conn.execute(query).fetchall()
            
            indexes = []
            for schema_name, table_name, index_name, index_def in result:
                # Determine index type from definition
                index_type = self._parse_index_type(index_def)
                is_unique = "UNIQUE INDEX" in index_def.upper()
                columns = self._parse_index_columns(index_def, table_name)
                
                indexes.append(IndexMetadata(
                    schema_name=schema_name,
                    table_name=table_name,
                    index_name=index_name,
                    index_type=index_type,
                    is_unique=is_unique,
                    columns=columns,
                ))
            
            return indexes
    
    def _parse_index_type(self, index_def: str) -> str:
        """Parse index type from PostgreSQL index definition."""
        index_def_lower = index_def.lower()
        if "using gin" in index_def_lower:
            return "GIN"
        elif "using gist" in index_def_lower:
            return "GIST"
        elif "using hash" in index_def_lower:
            return "HASH"
        elif "using brin" in index_def_lower:
            return "BRIN"
        else:
            return "BTREE"
    
    def _parse_index_columns(self, index_def: str, table_name: str) -> List[str]:
        """Parse column names from PostgreSQL index definition."""
        pattern = rf"{re.escape(table_name)}\s*\((.*?)\)"
        match = re.search(pattern, index_def, re.IGNORECASE)
        if match:
            columns_str = match.group(1)
            return [
                col.strip().strip('"').strip("'")
                for col in columns_str.split(",")
            ]
        return []

    def discover_roles(self, database_name: str) -> List[RoleMetadata]:
        """Discover PostgreSQL roles (excluding system roles)."""
        with self.engine.connect() as conn:
            query = text("""
                SELECT rolname, rolcanlogin, rolsuper
                FROM pg_roles
                WHERE rolname NOT LIKE 'pg_%'
                ORDER BY rolname
            """)
            result = conn.execute(query).fetchall()
            return [
                RoleMetadata(
                    role_name=row[0],
                    can_login=row[1],
                    is_superuser=row[2],
                )
                for row in result
            ]

    def discover_grants(self) -> List[GrantMetadata]:
        """Discover table-level grants (excluding system schemas)."""
        with self.engine.connect() as conn:
            query = text("""
                SELECT grantee, table_schema, table_name, privilege_type, is_grantable
                FROM information_schema.role_table_grants
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY grantee, table_schema, table_name, privilege_type
            """)
            result = conn.execute(query).fetchall()
            return [
                GrantMetadata(
                    grantee=row[0],
                    table_schema=row[1],
                    table_name=row[2],
                    privilege_type=row[3],
                    is_grantable=row[4] == "YES",
                )
                for row in result
            ]

    def sample_column_data(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        sample_size: int = 20,
    ) -> List[str]:
        """
        Sample column data using PostgreSQL syntax.
        """
        # Use proper quoting via SQLAlchemy's identifier preparer
        quoted_schema = self.quote_identifier(schema_name)
        quoted_table = self.quote_identifier(table_name)
        quoted_column = self.quote_identifier(column_name)

        query = text(f"""
            SELECT DISTINCT {quoted_column}::text as value
            FROM {quoted_schema}.{quoted_table}
            WHERE {quoted_column} IS NOT NULL
            LIMIT :sample_size
        """)

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"sample_size": sample_size})
                samples = [row[0] for row in result if row[0] is not None]
                return samples
        except Exception as e:
            logger.error(f"Failed to sample {schema_name}.{table_name}.{column_name}: {e}")
            return []

    # ========================================================================
    # Compliance Query Methods (PostgreSQL)
    # ========================================================================

    def generate_pk_select_sql(
        self,
        schema_name: str,
        table_name: str,
        pk_columns: List[str],
        sentinel_columns: List[str],
        sentinel_param_name: str = "primary_value",
    ) -> str:
        """
        Generate SQL to select primary keys from a sentinel table (depth 0).

        PostgreSQL syntax with double-quote identifiers.
        """
        table_fqn = self.format_table_fqn(schema_name, table_name)
        pk_cols = ", ".join(self.quote_identifier(c) for c in pk_columns)

        # Build WHERE clause for sentinel columns
        if len(sentinel_columns) == 1:
            where_clause = f"{self.quote_identifier(sentinel_columns[0])} = :{sentinel_param_name}"
        else:
            # Composite sentinel (rare but possible)
            conditions = [
                f"{self.quote_identifier(col)} = :{sentinel_param_name}"
                for col in sentinel_columns
            ]
            where_clause = " AND ".join(conditions)

        return f"SELECT {pk_cols} FROM {table_fqn} WHERE {where_clause}"

    def generate_pk_select_with_fk_sql(
        self,
        schema_name: str,
        table_name: str,
        pk_columns: List[str],
        fk_source_columns: List[str],
        fk_target_columns: List[str],
        parent_subquery: str,
    ) -> str:
        """
        Generate SQL to select primary keys from a child table via FK relationship.

        PostgreSQL supports tuple IN for composite keys:
            SELECT pk FROM child WHERE (fk1, fk2) IN (SELECT target1, target2 FROM parent ...)
        """
        table_fqn = self.format_table_fqn(schema_name, table_name)
        pk_cols = ", ".join(self.quote_identifier(c) for c in pk_columns)

        # Modify parent subquery to select only the target columns we need
        modified_subquery = self._modify_subquery_columns(parent_subquery, fk_target_columns)

        if len(fk_source_columns) == 1:
            # Simple single-column FK
            source_col = self.quote_identifier(fk_source_columns[0])
            return f"SELECT {pk_cols} FROM {table_fqn} WHERE {source_col} IN ({modified_subquery})"
        else:
            # Composite FK - PostgreSQL supports tuple IN
            source_tuple = ", ".join(self.quote_identifier(c) for c in fk_source_columns)
            return f"SELECT {pk_cols} FROM {table_fqn} WHERE ({source_tuple}) IN ({modified_subquery})"

    def generate_select_all_by_pk_sql(
        self,
        schema_name: str,
        table_name: str,
        pk_columns: List[str],
        pk_subquery: str,
    ) -> str:
        """
        Generate SQL to select all columns from rows matching a PK subquery.

        PostgreSQL supports tuple IN for composite PKs.
        """
        table_fqn = self.format_table_fqn(schema_name, table_name)

        if len(pk_columns) == 1:
            pk_col = self.quote_identifier(pk_columns[0])
            return f"SELECT * FROM {table_fqn} WHERE {pk_col} IN ({pk_subquery})"
        else:
            # Composite PK - use tuple IN
            pk_tuple = ", ".join(self.quote_identifier(c) for c in pk_columns)
            return f"SELECT * FROM {table_fqn} WHERE ({pk_tuple}) IN ({pk_subquery})"

    def generate_delete_by_pk_sql(
        self,
        schema_name: str,
        table_name: str,
        pk_columns: List[str],
        pk_subquery: str,
    ) -> str:
        """
        Generate SQL to delete rows matching a PK subquery.

        PostgreSQL supports tuple IN for composite PKs.
        """
        table_fqn = self.format_table_fqn(schema_name, table_name)

        if len(pk_columns) == 1:
            pk_col = self.quote_identifier(pk_columns[0])
            return f"DELETE FROM {table_fqn} WHERE {pk_col} IN ({pk_subquery})"
        else:
            # Composite PK - use tuple IN
            pk_tuple = ", ".join(self.quote_identifier(c) for c in pk_columns)
            return f"DELETE FROM {table_fqn} WHERE ({pk_tuple}) IN ({pk_subquery})"
