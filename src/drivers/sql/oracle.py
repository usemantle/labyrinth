"""
Oracle discovery driver.

Implements database discovery for Oracle using DBA_* data dictionary views.
Oracle 12c+ required for FETCH FIRST syntax support.

Key Differences from PostgreSQL/MySQL:
- Uses DBA_TABLES, DBA_TAB_COLUMNS instead of information_schema
- Requires DBA privileges for comprehensive discovery
- Supports tuple IN syntax for composite keys (like PostgreSQL)
- Uses TO_CHAR() for type casting instead of ::text or CAST AS CHAR
- Identifier quoting: double quotes (like PostgreSQL)
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


class OracleDiscoveryDriver(BaseDiscoveryDriver):
    """Oracle-specific discovery driver using DBA_* views."""

    SYSTEM_SCHEMAS: ClassVar[List[str]] = [
        # Core system schemas
        "SYS",
        "SYSTEM",

        # Database Vault
        "DVSYS",
        "DVF",
        "LBACSYS",

        # Audit and security
        "AUDSYS",
        "SYSBACKUP",
        "SYSDG",
        "SYSKM",
        "SYSRAC",
        "VECSYS",
        "DBSFWUSER",

        # Oracle Text (full-text search)
        "CTXSYS",

        # Spatial and multimedia
        "MDSYS",
        "ORDDATA",
        "ORDSYS",
        "SI_INFORMTN_SCHEMA",
        "SPATIAL_CSW_ADMIN_USR",
        "SPATIAL_WFS_ADMIN_USR",

        # OLAP and data mining
        "OLAPSYS",

        # XML DB
        "XDB",

        # Workspace Manager
        "WMSYS",

        # Oracle Enterprise Manager
        "DBSNMP",
        "SYSMAN",
        "MGMT_VIEW",

        # Oracle Streams and GoldenGate
        "GGSYS",
        "GSMADMIN_INTERNAL",
        "GSMCATUSER",
        "GSMUSER",

        # APEX (Application Express)
        "APEX_PUBLIC_USER",
        "FLOWS_FILES",
        "APEX_INSTANCE_ADMIN_USER",
        "APEX_LISTENER",
        "APEX_REST_PUBLIC_USER",

        # Oracle JVM
        "OJVMSYS",

        # Application Quality of Service
        "APPQOSSYS",

        # Other internal schemas
        "OUTLN",
        "ANONYMOUS",
        "DIP",
        "ORACLE_OCM",
        "REMOTE_SCHEDULER_AGENT",
        "SYS$UMF",
        "WKPROXY",
        "WKSYS",
        "WK_TEST",
        "EXFSYS",
        "MDDATA",
        "ORDPLUGINS",

        # Sample schemas (often present in dev/test)
        "HR",
        "OE",
        "PM",
        "IX",
        "SH",
        "BI",
        "SCOTT",
        "DEMO",
    ]

    def discover_schemas(self, database_name: str) -> List[SchemaMetadata]:
        """
        Discover all schemas (users) in the Oracle database.

        Uses DBA_USERS to find all user schemas. Requires DBA privileges.
        Excludes Oracle system schemas via SYSTEM_SCHEMAS constant.
        """
        with self.engine.connect() as conn:
            exclusion_list = self.get_system_schemas_sql_list()
            query = text(f"""
                SELECT username
                FROM dba_users
                WHERE username NOT IN ({exclusion_list})
                ORDER BY username
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
        """
        Discover all tables and views in an Oracle schema.

        Uses DBA_TABLES and DBA_VIEWS for discovery.
        """
        with self.engine.connect() as conn:
            # Oracle uses separate views for tables and views
            # TABLE_TYPE in DBA_TABLES doesn't exist, so we query both separately
            query = text("""
                SELECT table_name, 'BASE_TABLE' as table_type
                FROM dba_tables
                WHERE owner = :schema_name
                UNION ALL
                SELECT view_name as table_name, 'VIEW' as table_type
                FROM dba_views
                WHERE owner = :schema_name
                ORDER BY table_name
            """)
            result = conn.execute(query, {"schema_name": schema_name.upper()}).fetchall()

            return [
                TableMetadata(
                    table_name=row[0],
                    table_type=row[1],
                )
                for row in result
            ]

    def discover_columns(
        self,
        schema_name: str,
        table_name: str,
    ) -> List[ColumnMetadata]:
        """
        Discover all columns in an Oracle table.

        Uses DBA_TAB_COLUMNS for column metadata.
        """
        with self.engine.connect() as conn:
            query = text("""
                SELECT
                    column_name,
                    data_type,
                    nullable,
                    char_length,
                    data_precision,
                    data_scale
                FROM dba_tab_columns
                WHERE owner = :schema_name
                  AND table_name = :table_name
                ORDER BY column_id
            """)
            result = conn.execute(
                query,
                {
                    "schema_name": schema_name.upper(),
                    "table_name": table_name.upper()
                }
            ).fetchall()

            return [
                ColumnMetadata(
                    column_name=row[0],
                    data_type=row[1],
                    is_nullable=row[2] == "Y",  # Oracle uses 'Y'/'N'
                    character_maximum_length=row[3],
                    numeric_precision=row[4],
                    numeric_scale=row[5],
                )
                for row in result
            ]

    def discover_constraints(self) -> List[ConstraintMetadata]:
        """
        Discover PRIMARY KEY, UNIQUE, CHECK constraints.

        Uses DBA_CONSTRAINTS and DBA_CONS_COLUMNS.
        """
        with self.engine.connect() as conn:
            exclusion_list = self.get_system_schemas_sql_list()
            query = text(f"""
                SELECT
                    c.constraint_name,
                    c.constraint_type,
                    c.owner,
                    c.table_name,
                    cc.column_name
                FROM dba_constraints c
                LEFT JOIN dba_cons_columns cc
                  ON c.constraint_name = cc.constraint_name
                  AND c.owner = cc.owner
                WHERE c.owner NOT IN ({exclusion_list})
                  AND c.constraint_type IN ('P', 'U', 'C')
                ORDER BY c.constraint_name, cc.position
            """)
            result = conn.execute(query).fetchall()

            constraints = []
            for row in result:
                constraint_name, constraint_type, schema_name, table_name, column_name = row

                # Map Oracle constraint type codes
                # P = PRIMARY KEY, U = UNIQUE, C = CHECK (and NOT NULL)
                if constraint_type == "P":
                    ctype = "PRIMARY_KEY"
                elif constraint_type == "U":
                    ctype = "UNIQUE"
                elif constraint_type == "C":
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
        """
        Discover foreign key relationships.

        Uses DBA_CONSTRAINTS (constraint_type = 'R') and DBA_CONS_COLUMNS.
        """
        with self.engine.connect() as conn:
            exclusion_list = self.get_system_schemas_sql_list()
            query = text(f"""
                SELECT
                    fk.constraint_name,
                    fk.owner AS fk_schema,
                    fk.table_name AS fk_table,
                    fk_cols.column_name AS fk_column,
                    pk.owner AS ref_schema,
                    pk.table_name AS ref_table,
                    pk_cols.column_name AS ref_column,
                    fk_cols.position
                FROM dba_constraints fk
                JOIN dba_cons_columns fk_cols
                  ON fk.constraint_name = fk_cols.constraint_name
                  AND fk.owner = fk_cols.owner
                JOIN dba_constraints pk
                  ON fk.r_constraint_name = pk.constraint_name
                  AND fk.r_owner = pk.owner
                JOIN dba_cons_columns pk_cols
                  ON pk.constraint_name = pk_cols.constraint_name
                  AND pk.owner = pk_cols.owner
                  AND fk_cols.position = pk_cols.position
                WHERE fk.constraint_type = 'R'
                  AND fk.owner NOT IN ({exclusion_list})
                ORDER BY fk.constraint_name, fk_cols.position
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
        """
        Discover indexes using DBA_INDEXES and DBA_IND_COLUMNS.
        """
        with self.engine.connect() as conn:
            exclusion_list = self.get_system_schemas_sql_list()
            query = text(f"""
                SELECT
                    i.owner,
                    i.table_name,
                    i.index_name,
                    i.index_type,
                    i.uniqueness,
                    ic.column_name,
                    ic.column_position
                FROM dba_indexes i
                JOIN dba_ind_columns ic
                  ON i.index_name = ic.index_name
                  AND i.owner = ic.index_owner
                WHERE i.owner NOT IN ({exclusion_list})
                ORDER BY i.owner, i.table_name, i.index_name, ic.column_position
            """)
            result = conn.execute(query).fetchall()

            # Group by (schema, table, index) to build column lists
            index_map = {}
            for row in result:
                schema_name, table_name, index_name, index_type, uniqueness, column_name, position = row
                key = (schema_name, table_name, index_name)

                if key not in index_map:
                    index_map[key] = {
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "index_name": index_name,
                        "index_type": self._normalize_index_type(index_type),
                        "is_unique": uniqueness == "UNIQUE",
                        "columns": [],
                    }
                index_map[key]["columns"].append(column_name)

            return [
                IndexMetadata(**data)
                for data in index_map.values()
            ]

    def _normalize_index_type(self, oracle_index_type: str) -> str:
        """
        Normalize Oracle index type to common format.

        Oracle index types:
        - NORMAL (B-tree)
        - BITMAP
        - FUNCTION-BASED NORMAL
        - FUNCTION-BASED BITMAP
        - DOMAIN (domain index)
        """
        if oracle_index_type.startswith("FUNCTION-BASED"):
            return "FUNCTION_BASED"
        elif oracle_index_type == "BITMAP":
            return "BITMAP"
        elif oracle_index_type == "DOMAIN":
            return "DOMAIN"
        else:
            # NORMAL and others default to BTREE
            return "BTREE"

    def sample_column_data(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        sample_size: int = 20,
    ) -> List[str]:
        """
        Sample column data using Oracle syntax.

        Uses TO_CHAR() for type casting and FETCH FIRST for pagination (Oracle 12c+).
        """
        # Use proper quoting via SQLAlchemy's identifier preparer
        quoted_schema = self.quote_identifier(schema_name)
        quoted_table = self.quote_identifier(table_name)
        quoted_column = self.quote_identifier(column_name)

        # Oracle uses TO_CHAR() for type casting and FETCH FIRST for pagination
        query = text(f"""
            SELECT DISTINCT TO_CHAR({quoted_column}) as value
            FROM {quoted_schema}.{quoted_table}
            WHERE {quoted_column} IS NOT NULL
            FETCH FIRST :sample_size ROWS ONLY
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
    # Compliance Query Methods (Oracle)
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

        Oracle syntax with double-quote identifiers (like PostgreSQL).
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

        Oracle supports tuple IN for composite keys (like PostgreSQL):
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
            # Composite FK - Oracle supports tuple IN (like PostgreSQL)
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

        Oracle supports tuple IN for composite PKs (like PostgreSQL).
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

        Oracle supports tuple IN for composite PKs (like PostgreSQL).
        """
        table_fqn = self.format_table_fqn(schema_name, table_name)

        if len(pk_columns) == 1:
            pk_col = self.quote_identifier(pk_columns[0])
            return f"DELETE FROM {table_fqn} WHERE {pk_col} IN ({pk_subquery})"
        else:
            # Composite PK - use tuple IN
            pk_tuple = ", ".join(self.quote_identifier(c) for c in pk_columns)
            return f"DELETE FROM {table_fqn} WHERE ({pk_tuple}) IN ({pk_subquery})"
