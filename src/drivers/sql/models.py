"""
Typed models for database discovery driver return types.

These Pydantic models provide a consistent interface for discovery
data across different database drivers (PostgreSQL, MySQL, etc.).
"""

from typing import List, Optional
from pydantic import BaseModel


class SchemaMetadata(BaseModel):
    """Metadata for a discovered schema/database."""
    schema_name: str
    database_name: str


class TableMetadata(BaseModel):
    """Metadata for a discovered table."""
    table_name: str
    table_type: str  # 'BASE_TABLE' | 'VIEW'


class ColumnMetadata(BaseModel):
    """Metadata for a discovered column."""
    column_name: str
    data_type: str
    is_nullable: bool
    character_maximum_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None


class ConstraintMetadata(BaseModel):
    """Metadata for a discovered constraint (PK, UNIQUE, CHECK)."""
    constraint_name: str
    constraint_type: str  # 'PRIMARY_KEY' | 'UNIQUE' | 'CHECK'
    schema_name: str
    table_name: str
    column_name: str


class ForeignKeyMetadata(BaseModel):
    """Metadata for a discovered foreign key relationship."""
    constraint_name: str
    fk_schema: str
    fk_table: str
    fk_column: str
    ref_schema: str
    ref_table: str
    ref_column: str
    ordinal_position: int


class IndexMetadata(BaseModel):
    """Metadata for a discovered index."""
    schema_name: str
    table_name: str
    index_name: str
    index_type: str  # 'BTREE' | 'HASH' | 'GIN' | 'GIST' | 'BRIN'
    is_unique: bool
    columns: List[str]


# ============================================================================
# Compliance Query Models
# ============================================================================


class ComplianceQueryResult(BaseModel):
    """Result of executing a compliance query (SELECT or DELETE)."""
    rows_affected: int
    data: Optional[List[dict]] = None  # Only populated for SELECT queries


class PKSelectQuery(BaseModel):
    """
    A query that selects primary keys from a table based on sentinel filtering.

    This is the building block for compliance operations - it identifies which
    rows in a table are related to a sentinel value.
    """
    table_fqn: str
    sql: str
    parameters: dict
