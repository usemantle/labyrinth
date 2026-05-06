"""
Database discovery drivers.

Provides a unified interface for database discovery across different
database types (PostgreSQL, MySQL, etc.).

Usage:
    from labyrinth.drivers import BaseDiscoveryDriver
    
    # Get the appropriate driver for your datastore type
    driver = BaseDiscoveryDriver.get_driver("POSTGRES", engine)
    
    # Use driver methods for discovery
    schemas = driver.discover_schemas("mydb")
    tables = driver.discover_tables("public")
"""

from labyrinth.drivers.sql.base import BaseDiscoveryDriver, UnsupportedDatastoreError
from labyrinth.drivers.sql.models import (
    SchemaMetadata,
    TableMetadata,
    ColumnMetadata,
    ConstraintMetadata,
    ForeignKeyMetadata,
    IndexMetadata,
    ComplianceQueryResult,
    PKSelectQuery,
)
from labyrinth.drivers.sql.postgres import PostgresDiscoveryDriver
from labyrinth.drivers.sql.mysql import MySQLDiscoveryDriver

__all__ = [
    # Base class
    "BaseDiscoveryDriver",
    "UnsupportedDatastoreError",
    # Concrete drivers
    "PostgresDiscoveryDriver",
    "MySQLDiscoveryDriver",
    # Discovery models
    "SchemaMetadata",
    "TableMetadata",
    "ColumnMetadata",
    "ConstraintMetadata",
    "ForeignKeyMetadata",
    "IndexMetadata",
    # Compliance models
    "ComplianceQueryResult",
    "PKSelectQuery",
]
