"""
MySQL discovery driver.

Implements database discovery for MySQL using information_schema tables.
Note: In MySQL, databases and schemas are synonymous.
"""

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
    IndexMetadata,
)

logger = logging.getLogger(__name__)


class MySQLDiscoveryDriver(BaseDiscoveryDriver):
    """MySQL-specific discovery driver."""
    
    SYSTEM_SCHEMAS: ClassVar[List[str]] = [
        "information_schema",
        "mysql",
        "performance_schema",
        "sys",
    ]
    
    def discover_schemas(self, database_name: str) -> List[SchemaMetadata]:
        """
        Discover all schemas in MySQL.

        In MySQL, databases ARE schemas. We return the connected database
        as the only schema for consistency with PostgreSQL's model.
        """
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
        """Discover all tables in a MySQL database/schema."""
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
        """Discover all columns in a MySQL table."""
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
        """Discover PRIMARY KEY, UNIQUE constraints in MySQL."""
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
                  AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
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
        """Discover foreign key relationships in MySQL."""
        with self.engine.connect() as conn:
            exclusion_list = self.get_system_schemas_sql_list()
            query = text(f"""
                SELECT
                    kcu.constraint_name,
                    kcu.table_schema AS fk_schema,
                    kcu.table_name AS fk_table,
                    kcu.column_name AS fk_column,
                    kcu.referenced_table_schema AS ref_schema,
                    kcu.referenced_table_name AS ref_table,
                    kcu.referenced_column_name AS ref_column,
                    kcu.ordinal_position
                FROM information_schema.key_column_usage kcu
                WHERE kcu.referenced_table_name IS NOT NULL
                  AND kcu.table_schema NOT IN ({exclusion_list})
                ORDER BY kcu.constraint_name, kcu.ordinal_position
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
        """Discover indexes using information_schema.statistics."""
        with self.engine.connect() as conn:
            exclusion_list = self.get_system_schemas_sql_list()
            query = text(f"""
                SELECT
                    table_schema,
                    table_name,
                    index_name,
                    index_type,
                    non_unique,
                    column_name,
                    seq_in_index
                FROM information_schema.statistics
                WHERE table_schema NOT IN ({exclusion_list})
                ORDER BY table_schema, table_name, index_name, seq_in_index
            """)
            result = conn.execute(query).fetchall()
            
            # Group by (schema, table, index) to build column lists
            index_map = {}
            for row in result:
                schema_name, table_name, index_name, index_type, non_unique, column_name, seq = row
                key = (schema_name, table_name, index_name)
                
                if key not in index_map:
                    index_map[key] = {
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "index_name": index_name,
                        "index_type": self._normalize_index_type(index_type),
                        "is_unique": non_unique == 0,
                        "columns": [],
                    }
                index_map[key]["columns"].append(column_name)
            
            return [
                IndexMetadata(**data)
                for data in index_map.values()
            ]
    
    def _normalize_index_type(self, mysql_index_type: str) -> str:
        """Normalize MySQL index type to common format."""
        if mysql_index_type in ("BTREE", "HASH", "FULLTEXT", "SPATIAL"):
            return mysql_index_type
        return "BTREE"

    def sample_column_data(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        sample_size: int = 20,
    ) -> List[str]:
        """
        Sample column data using MySQL syntax.
        """
        # Use proper quoting via SQLAlchemy's identifier preparer
        quoted_schema = self.quote_identifier(schema_name)
        quoted_table = self.quote_identifier(table_name)
        quoted_column = self.quote_identifier(column_name)

        # CAST to CHAR ensures consistent string output
        query = text(f"""
            SELECT DISTINCT CAST({quoted_column} AS CHAR) as value
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
    # Compliance Query Methods (MySQL)
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

        MySQL syntax with backtick identifiers.
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

        MySQL does NOT support tuple IN with subqueries for composite keys.
        For composite FKs, we use EXISTS with a correlated subquery:
            SELECT pk FROM child t WHERE EXISTS (
                SELECT 1 FROM (parent_subquery) p
                WHERE t.fk1 = p.target1 AND t.fk2 = p.target2
            )
        """
        table_fqn = self.format_table_fqn(schema_name, table_name)
        pk_cols = ", ".join(self.quote_identifier(c) for c in pk_columns)

        # Modify parent subquery to select only the target columns we need
        modified_subquery = self._modify_subquery_columns(parent_subquery, fk_target_columns)

        if len(fk_source_columns) == 1:
            # Simple single-column FK - MySQL supports simple IN
            source_col = self.quote_identifier(fk_source_columns[0])
            return f"SELECT {pk_cols} FROM {table_fqn} WHERE {source_col} IN ({modified_subquery})"
        else:
            # Composite FK - use EXISTS with correlated subquery
            # Note: We alias the main table as 't' to avoid ambiguity
            conditions = [
                f"t.{self.quote_identifier(src)} = p.{self.quote_identifier(tgt)}"
                for src, tgt in zip(fk_source_columns, fk_target_columns)
            ]
            condition_clause = " AND ".join(conditions)
            return (
                f"SELECT {pk_cols} FROM {table_fqn} t "
                f"WHERE EXISTS (SELECT 1 FROM ({modified_subquery}) p WHERE {condition_clause})"
            )

    def generate_select_all_by_pk_sql(
        self,
        schema_name: str,
        table_name: str,
        pk_columns: List[str],
        pk_subquery: str,
    ) -> str:
        """
        Generate SQL to select all columns from rows matching a PK subquery.

        MySQL does NOT support tuple IN with subqueries, so for composite PKs
        we use EXISTS.
        """
        table_fqn = self.format_table_fqn(schema_name, table_name)

        if len(pk_columns) == 1:
            pk_col = self.quote_identifier(pk_columns[0])
            return f"SELECT * FROM {table_fqn} WHERE {pk_col} IN ({pk_subquery})"
        else:
            # Composite PK - use EXISTS
            # The pk_subquery already returns the PK columns, wrap it as derived table
            conditions = [
                f"t.{self.quote_identifier(c)} = p.{self.quote_identifier(c)}"
                for c in pk_columns
            ]
            condition_clause = " AND ".join(conditions)
            return (
                f"SELECT * FROM {table_fqn} t "
                f"WHERE EXISTS (SELECT 1 FROM ({pk_subquery}) p WHERE {condition_clause})"
            )

    def generate_delete_by_pk_sql(
        self,
        schema_name: str,
        table_name: str,
        pk_columns: List[str],
        pk_subquery: str,
    ) -> str:
        """
        Generate SQL to delete rows matching a PK subquery.

        MySQL does NOT support tuple IN with subqueries, so for composite PKs
        we use a multi-table DELETE syntax or EXISTS in a subquery.

        Note: MySQL DELETE with subquery referencing the same table requires
        wrapping the subquery in a derived table.
        """
        table_fqn = self.format_table_fqn(schema_name, table_name)

        if len(pk_columns) == 1:
            pk_col = self.quote_identifier(pk_columns[0])
            # MySQL requires wrapping same-table subquery in derived table
            return f"DELETE FROM {table_fqn} WHERE {pk_col} IN (SELECT * FROM ({pk_subquery}) AS _pk_subquery)"
        else:
            # Composite PK - use multi-table DELETE with JOIN
            # DELETE t FROM table t WHERE EXISTS (SELECT 1 FROM (subquery) p WHERE t.pk1 = p.pk1 AND ...)
            conditions = [
                f"t.{self.quote_identifier(c)} = p.{self.quote_identifier(c)}"
                for c in pk_columns
            ]
            condition_clause = " AND ".join(conditions)
            return (
                f"DELETE t FROM {table_fqn} t "
                f"WHERE EXISTS (SELECT 1 FROM ({pk_subquery}) p WHERE {condition_clause})"
            )
