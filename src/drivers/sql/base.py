"""
Base discovery driver abstract class.

Provides a unified interface for database discovery across different
database types (PostgreSQL, MySQL, etc.).
"""
import abc
import re
import logging
from typing import ClassVar, List, Optional, TYPE_CHECKING

from sqlalchemy.engine import Engine
from sqlalchemy import create_engine

from src.drivers.sql.models import (
    SchemaMetadata,
    TableMetadata,
    ColumnMetadata,
    ConstraintMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    ComplianceQueryResult,
    PKSelectQuery,
)
if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class UnsupportedDatastoreError(Exception):
    """Raised when attempting to use an unsupported datastore type."""
    pass


class BaseDiscoveryDriver(abc.ABC):
    """
    Abstract base class for database discovery drivers.
    
    Each driver encapsulates database-specific SQL queries and behaviors
    for schema/table/column discovery. The engine is an instance attribute
    for connection reuse and can be None for testing.
    
    Usage:
        # Production: get driver with engine
        driver = BaseDiscoveryDriver.get_driver("POSTGRES", engine)
        schemas = driver.discover_schemas("mydb")
        
        # Testing: get driver without engine (for mocking)
        driver = BaseDiscoveryDriver.get_driver("POSTGRES")
        driver.engine = mock_engine
    """
    
    # System schemas to exclude from discovery (override in subclasses)
    SYSTEM_SCHEMAS: ClassVar[List[str]] = []
    
    def __init__(self, engine: Optional[Engine] = None):
        """
        Initialize driver with optional engine.
        
        Args:
            engine: SQLAlchemy engine for database connections.
                    Optional for IoC/testing purposes.
        """
        self.engine = engine
    
    @classmethod
    def get_driver(
        cls,
        datastore_type: str,
        connection_string: str,
        engine: Optional[Engine] = None,
    ) -> "BaseDiscoveryDriver":
        """
        Factory classmethod to get the appropriate driver for a datastore type.

        Args:
            datastore_type: Type of datastore ('POSTGRES', 'MYSQL', 'ORACLE', etc.)
            engine: Optional SQLAlchemy engine

        Returns:
            Appropriate driver instance

        Raises:
            UnsupportedDatastoreError: If datastore type has no driver
        """

        # Import drivers here to avoid circular import
        from src.drivers.sql.postgres import PostgresDiscoveryDriver
        from src.drivers.sql.mysql import MySQLDiscoveryDriver
        from src.drivers.sql.oracle import OracleDiscoveryDriver

        dt = datastore_type.upper()

        # Check for supported types before creating engine
        if dt not in ("POSTGRES", "MYSQL", "ORACLE"):
            raise UnsupportedDatastoreError(
                f"No discovery driver for datastore type: {datastore_type}. "
                f"Supported types: POSTGRES, MYSQL, ORACLE"
            )

        customer_engine = engine or create_engine(
            connection_string,
            isolation_level="READ COMMITTED"
        )

        if dt == "POSTGRES":
            return PostgresDiscoveryDriver(customer_engine)
        elif dt == "MYSQL":
            return MySQLDiscoveryDriver(customer_engine)
        else:  # dt == "ORACLE"
            return OracleDiscoveryDriver(customer_engine)

    @staticmethod
    def build_connection_string(
        datastore_type: str,
        username: str,
        password: str,
        host: str,
        port: int,
        database_name: str,
        metadata: dict = None,
    ) -> str:
        datastore_type = datastore_type.upper()
        if datastore_type == "POSTGRES":
            # PostgreSQL connection string
            # Format: postgresql://username:password@host:port/database
            return f"postgresql://{username}:{password}@{host}:{port}/{database_name}"

        elif datastore_type == "MYSQL":
            # MySQL connection string (using pymysql driver)
            # Format: mysql+pymysql://username:password@host:port/database
            return (
                f"mysql+pymysql://{username}:{password}@{host}:{port}/{database_name}"
            )

        elif datastore_type == "ORACLE":
            # Oracle connection string (using python-oracledb driver)
            # Format: oracle+oracledb://username:password@host:port/?service_name=service_name
            # Note: service_name is the Oracle service name, defaults to database_name
            # Can be overridden via metadata dict
            service_name = metadata.get("service_name", database_name) if metadata else database_name
            return f"oracle+oracledb://{username}:{password}@{host}:{port}/?service_name={service_name}"

        elif datastore_type == "MSSQL":
            # Microsoft SQL Server connection string (using pyodbc driver)
            # Format: mssql+pyodbc://username:password@host:port/database?driver=ODBC+Driver+17+for+SQL+Server
            return (
                f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database_name}"
                f"?driver=ODBC+Driver+17+for+SQL+Server"
            )

        else:
            raise UnsupportedDatastoreError(
                f"Datastore type {datastore_type} is not currently supported. "
                f"Supported types: POSTGRES, MYSQL, ORACLE, SNOWFLAKE, MSSQL"
            )
    
    def get_system_schemas_sql_list(self) -> str:
        """
        Generate SQL-safe comma-separated list of system schemas for exclusion.

        Returns:
            String like: 'SYS', 'SYSTEM', 'CTXSYS'
        """
        return ", ".join(f"'{schema}'" for schema in self.SYSTEM_SCHEMAS)

    def should_exclude_schema(
        self,
        schema_name: str,
        excluded_patterns: List[str],
    ) -> bool:
        """
        Check if a schema should be excluded from discovery.

        Args:
            schema_name: Name of the schema to check
            excluded_patterns: User-defined glob patterns to exclude

        Returns:
            True if schema should be excluded, False otherwise
        """
        # Check against system schemas
        if schema_name in self.SYSTEM_SCHEMAS:
            return True

        # Check against user-defined patterns (supports * wildcard)
        for pattern in excluded_patterns:
            regex_pattern = pattern.replace("*", ".*")
            if re.fullmatch(regex_pattern, schema_name, re.IGNORECASE):
                return True

        return False
    
    def should_exclude_table(
        self,
        table_name: str,
        excluded_patterns: List[str],
    ) -> bool:
        """
        Check if a table should be excluded from discovery.
        
        Args:
            table_name: Name of the table to check
            excluded_patterns: User-defined glob patterns to exclude
            
        Returns:
            True if table should be excluded, False otherwise
        """
        for pattern in excluded_patterns:
            regex_pattern = pattern.replace("*", ".*")
            if re.fullmatch(regex_pattern, table_name, re.IGNORECASE):
                return True
        return False
    
    @abc.abstractmethod
    def discover_schemas(self, database_name: str) -> List[SchemaMetadata]:
        """
        Discover all schemas in the database.
        
        Args:
            database_name: Name of the database
            
        Returns:
            List of SchemaMetadata for discovered schemas
        """
        pass
    
    @abc.abstractmethod
    def discover_tables(self, schema_name: str) -> List[TableMetadata]:
        """
        Discover all tables in a schema.
        
        Args:
            schema_name: Name of the schema
            
        Returns:
            List of TableMetadata for discovered tables
        """
        pass
    
    @abc.abstractmethod
    def discover_columns(
        self,
        schema_name: str,
        table_name: str,
    ) -> List[ColumnMetadata]:
        """
        Discover all columns in a table.
        
        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            
        Returns:
            List of ColumnMetadata for discovered columns
        """
        pass
    
    @abc.abstractmethod
    def discover_constraints(self) -> List[ConstraintMetadata]:
        """
        Discover all constraints (PRIMARY KEY, UNIQUE, CHECK).
        
        Returns:
            List of ConstraintMetadata for discovered constraints
        """
        pass
    
    @abc.abstractmethod
    def discover_foreign_keys(self) -> List[ForeignKeyMetadata]:
        """
        Discover all foreign key relationships.
        
        Returns:
            List of ForeignKeyMetadata for discovered FKs
        """
        pass
    
    @abc.abstractmethod
    def discover_indexes(self) -> List[IndexMetadata]:
        """
        Discover all indexes.
        
        Returns:
            List of IndexMetadata for discovered indexes
        """
        pass

    @abc.abstractmethod
    def sample_column_data(
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        sample_size: int = 20,
    ) -> List[str]:
        """
        Sample distinct non-null values from a column.

        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            column_name: Name of the column
            sample_size: Number of distinct samples to collect (default 20)

        Returns:
            List of sample values as strings
        """
        pass

    # ========================================================================
    # Compliance Query Methods
    # ========================================================================

    def quote_identifier(self, identifier: str) -> str:
        """
        Quote a SQL identifier using the dialect's identifier preparer.

        Uses SQLAlchemy's dialect-aware quoting for proper escaping.

        Args:
            identifier: The identifier to quote (table name, column name, etc.)

        Returns:
            Properly quoted identifier string
        """
        if self.engine is None:
            raise ValueError("Engine required for identifier quoting")
        return self.engine.dialect.identifier_preparer.quote_identifier(identifier)

    def format_table_fqn(self, schema_name: str, table_name: str) -> str:
        """
        Format a fully qualified table name with proper quoting.

        Args:
            schema_name: Name of the schema
            table_name: Name of the table

        Returns:
            Properly quoted fully qualified table name (e.g., "schema"."table")
        """
        return f"{self.quote_identifier(schema_name)}.{self.quote_identifier(table_name)}"

    @abc.abstractmethod
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

        This is for tables that directly contain the sentinel column.

        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            pk_columns: List of primary key column names
            sentinel_columns: List of sentinel column names to filter on
            sentinel_param_name: Name of the SQL parameter for the sentinel value

        Returns:
            SQL SELECT statement string
        """
        pass

    @abc.abstractmethod
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

        This is for tables at depth > 0 that are reached via foreign key relationships.

        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            pk_columns: List of primary key column names
            fk_source_columns: List of FK column names in this (child) table
            fk_target_columns: List of FK target column names in the parent table
            parent_subquery: SQL subquery that returns parent table's target columns

        Returns:
            SQL SELECT statement string
        """
        pass

    @abc.abstractmethod
    def generate_select_all_by_pk_sql(
        self,
        schema_name: str,
        table_name: str,
        pk_columns: List[str],
        pk_subquery: str,
    ) -> str:
        """
        Generate SQL to select all columns from rows matching a PK subquery.

        Used for DSAR export to fetch full row data.

        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            pk_columns: List of primary key column names
            pk_subquery: SQL subquery that returns the primary key values

        Returns:
            SQL SELECT * statement string
        """
        pass

    @abc.abstractmethod
    def generate_delete_by_pk_sql(
        self,
        schema_name: str,
        table_name: str,
        pk_columns: List[str],
        pk_subquery: str,
    ) -> str:
        """
        Generate SQL to delete rows matching a PK subquery.

        Used for data deletion compliance operations.

        Args:
            schema_name: Name of the schema
            table_name: Name of the table
            pk_columns: List of primary key column names
            pk_subquery: SQL subquery that returns the primary key values

        Returns:
            SQL DELETE statement string
        """
        pass

    def execute_compliance_query(
        self,
        sql: str,
        parameters: dict,
        fetch_results: bool = True,
    ) -> ComplianceQueryResult:
        """
        Execute a compliance query and return results.

        Args:
            sql: The SQL query to execute
            parameters: Dictionary of query parameters
            fetch_results: If True, fetch and return row data (for SELECT)

        Returns:
            ComplianceQueryResult with rows_affected and optional data
        """
        if self.engine is None:
            raise ValueError("Engine required for query execution")

        from sqlalchemy import text

        with self.engine.connect() as conn:
            result = conn.execute(text(sql), parameters)

            if fetch_results:
                rows = result.fetchall()
                columns = list(result.keys())
                data = [dict(zip(columns, row)) for row in rows]
                return ComplianceQueryResult(rows_affected=len(rows), data=data)
            else:
                conn.commit()
                return ComplianceQueryResult(rows_affected=result.rowcount)

    def _modify_subquery_columns(
        self,
        subquery: str,
        target_columns: List[str],
    ) -> str:
        """
        Modify a subquery to select specific columns instead of the original columns.

        This is shared ANSI SQL logic needed when the parent's PK columns differ
        from what we need for the FK join.

        Args:
            subquery: Original SQL subquery
            target_columns: List of column names to select instead

        Returns:
            Modified subquery with updated SELECT clause
        """
        # The subquery format is: SELECT <cols> FROM <table> WHERE <condition>
        # We need to replace the SELECT clause
        pattern = r"SELECT\s+(.+?)\s+FROM"
        new_cols = ", ".join(self.quote_identifier(c) for c in target_columns)
        modified = re.sub(pattern, f"SELECT {new_cols} FROM", subquery,
                         count=1, flags=re.IGNORECASE)
        return modified
