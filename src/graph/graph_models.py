from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field
from typing import Any, ClassVar


class RelationType(str, enum.Enum):
    """
    The type of relationship an edge represents.

    CONTAINS: Structural containment between parent and child resources.
        e.g., database → schema, schema → table, table → column,
              s3-bucket → s3-prefix, github-repo → file

    HOSTS: Infrastructure resource that runs/hosts a data resource.
        e.g., rds-cluster → database, ec2-instance → self-hosted-postgres,
              cloud-sql-instance → database

    DATA_TO_DATA: Leaf data resource to leaf data resource.
        e.g., orders.customer_id → customers.id (foreign key),
              attachments.s3_uri → s3://user-uploads/ (AI-inferred)

    CODE_TO_DATA: Code function to leaf data resource.
        e.g., upload_service.store_attachment() → attachments.s3_uri

    CODE_TO_CODE: Code function to code function.
        e.g., create_datastore() → GraphTraversalService()

    PRINCIPAL_TO_DATA: IAM principal to container-level resource.
        e.g., arn:aws:iam::role/api-service → arn:aws:dynamodb::orders
    """

    CONTAINS = "CONTAINS"
    HOSTS = "HOSTS"
    DATA_TO_DATA = "DATA_TO_DATA"
    CODE_TO_DATA = "CODE_TO_DATA"
    CODE_TO_CODE = "CODE_TO_CODE"
    DEPENDS_ON = "DEPENDS_ON"
    PRINCIPAL_TO_DATA = "PRINCIPAL_TO_DATA"


class NodeMetadataKey(str, enum.Enum):
    """Valid keys for Node metadata dictionaries."""

    # ── Database discovery ─────────────────────────────────────────
    DATABASE_NAME = "database_name"
    HOST = "host"
    PORT = "port"
    SCHEMA_NAME = "schema_name"
    TABLE_NAME = "table_name"
    TABLE_TYPE = "table_type"
    COLUMN_NAME = "column_name"
    DATA_TYPE = "data_type"
    NULLABLE = "nullable"
    ORDINAL_POSITION = "ordinal_position"

    # ── Codebase discovery ─────────────────────────────────────────
    REPO_NAME = "repo_name"
    FILE_COUNT = "file_count"
    FILE_PATH = "file_path"
    LANGUAGE = "language"
    SIZE_BYTES = "size_bytes"
    CLASS_NAME = "class_name"
    START_LINE = "start_line"
    END_LINE = "end_line"
    BASE_CLASSES = "base_classes"
    FUNCTION_NAME = "function_name"
    IS_METHOD = "is_method"
    RECEIVER_TYPE = "receiver_type"

    # ── GitHub ─────────────────────────────────────────────────────
    ORG_NAME = "org_name"
    REPO_COUNT = "repo_count"
    FULL_NAME = "full_name"
    PRIVATE = "private"
    DEFAULT_BRANCH = "default_branch"
    ARCHIVED = "archived"
    CLONE_URL = "clone_url"

    # ── Git tracking ───────────────────────────────────────────────
    SCANNED_COMMIT = "scanned_commit"
    GITHUB_ORG = "github_org"
    REPO_URL = "repo_url"

    # ── ORM plugin ─────────────────────────────────────────────────
    ORM_TABLE = "orm_table"
    ORM_FRAMEWORK = "orm_framework"
    ORM_OPERATIONS = "orm_operations"
    ORM_OPERATION_TYPE = "orm_operation_type"
    ORM_MODELS = "orm_models"

    # ── FastAPI plugin ──────────────────────────────────────────────
    HTTP_METHOD = "http_method"
    ROUTE_PATH = "route_path"
    FULL_ROUTE_PATH = "full_route_path"
    API_FRAMEWORK = "api_framework"
    ROUTER_VARIABLE = "router_variable"
    AUTH_SCHEME = "auth_scheme"
    AUTH_SCHEME_VAR = "auth_scheme_var"

    # ── Boto3 S3 plugin ──────────────────────────────────────────────
    AWS_S3_CLIENT = "aws_s3_client"
    AWS_S3_OPERATIONS = "aws_s3_operations"
    AWS_S3_OPERATION_TYPE = "aws_s3_operation_type"

    # ── Security analysis ────────────────────────────────────────────
    IO_DIRECTION = "io_direction"
    IO_TYPE = "io_type"
    CVE_IDS = "cve_ids"
    DATA_SENSITIVITY = "data_sensitivity"

    # ── Database roles ────────────────────────────────────────────────
    ROLE_NAME = "role_name"
    ROLE_LOGIN = "role_login"
    ROLE_SUPERUSER = "role_superuser"

    # ── Dependency management ────────────────────────────────────────
    PACKAGE_NAME = "package_name"
    PACKAGE_VERSION = "package_version"
    PACKAGE_ECOSYSTEM = "package_ecosystem"

    # ── AWS / S3 ─────────────────────────────────────────────────────
    ARN = "arn"
    ACCOUNT_ID = "account_id"
    REGION = "region"
    BUCKET_NAME = "bucket_name"
    PATH_PATTERN = "path_pattern"
    OBJECT_COUNT = "object_count"
    SAMPLE_KEYS = "sample_keys"
    PARTITION_TYPE = "partition_type"


class EdgeMetadataKey(str, enum.Enum):
    """Valid keys for Edge metadata dictionaries."""

    # ── Foreign key ────────────────────────────────────────────────
    CONSTRAINT_NAME = "constraint_name"
    ORDINAL_POSITION = "ordinal_position"

    # ── Code-to-data linking ───────────────────────────────────────
    DETECTION_METHOD = "detection_method"
    CONFIDENCE = "confidence"
    ORM_FRAMEWORK = "orm_framework"
    ORM_CLASS = "orm_class"
    TABLE_NAME = "table_name"
    REFERENCED_MODEL = "referenced_model"

    # ── Code-to-code linking ────────────────────────────────────────
    CALL_TYPE = "call_type"

    # ── Dependency linking ───────────────────────────────────────────
    IMPORT_NAME = "import_name"

    # ── Database grants ──────────────────────────────────────────────
    PRIVILEGE = "privilege"


class _EnumKeyDict:
    """Dict wrapper that validates keys against a ``str`` enum."""

    _valid_keys: frozenset[str] = frozenset()

    def __init__(self, initial: dict[str, Any] | None = None, /, **kwargs: Any):
        self._data: dict[str, Any] = {}
        if initial:
            for k, v in initial.items():
                self[k] = v
        for k, v in kwargs.items():
            self[k] = v

    def _resolve(self, key: Any) -> str:
        k = key.value if isinstance(key, enum.Enum) else key
        if k not in self._valid_keys:
            raise KeyError(
                f"Unknown metadata key: {k!r}. "
                f"Add it to the key enum before use."
            )
        return k

    def __getitem__(self, key: Any) -> Any:
        return self._data[self._resolve(key)]

    def __setitem__(self, key: Any, value: Any) -> None:
        self._data[self._resolve(key)] = value

    def __contains__(self, key: Any) -> bool:
        try:
            return self._resolve(key) in self._data
        except KeyError:
            return False

    def get(self, key: Any, default: Any = None) -> Any:
        try:
            return self._data.get(self._resolve(key), default)
        except KeyError:
            return default

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._data!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _EnumKeyDict):
            return self._data == other._data
        return NotImplemented

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def items(self):
        return self._data.items()

    def keys(self):
        return self._data.keys()

    def values(self):
        return self._data.values()

    def update(self, other=None, /, **kwargs):
        if other:
            for k, v in (other.items() if hasattr(other, "items") else other):
                self[k] = v
        for k, v in kwargs.items():
            self[k] = v


class NodeMetadata(_EnumKeyDict):
    """Node metadata dict validated against :class:`NodeMetadataKey`."""

    _valid_keys = frozenset(k.value for k in NodeMetadataKey)


class EdgeMetadata(_EnumKeyDict):
    """Edge metadata dict validated against :class:`EdgeMetadataKey`."""

    _valid_keys = frozenset(k.value for k in EdgeMetadataKey)


class URN:
    """
    Canonical resource identifier.

    Format: urn:{provider}:{service}:{account}:{region}:{path}

    Examples:
        urn:aws:rds:123456789:us-east-1:mydb/public/users/email
        urn:aws:s3:123456789:us-east-1:my-bucket/uploads/
        urn:github:repo:my-org:::my-org/api-service/src/models/user.py
    """

    def __init__(self, urn: str):
        self._urn = urn

    @property
    def provider(self) -> str:
        return self._parts()[0]

    @property
    def service(self) -> str:
        return self._parts()[1]

    @property
    def account(self) -> str:
        return self._parts()[2]

    @property
    def region(self) -> str:
        return self._parts()[3]

    @property
    def path(self) -> str:
        return self._parts()[4]

    def parent(self) -> URN | None:
        """Return the parent URN by trimming the last path segment."""
        path = self.path
        if "/" not in path:
            return None
        parent_path = path.rsplit("/", 1)[0]
        return URN(f"urn:{self.provider}:{self.service}:{self.account}:{self.region}:{parent_path}")

    def _parts(self) -> tuple[str, str, str, str, str]:
        # urn:{provider}:{service}:{account}:{region}:{path}
        # Split into at most 6 parts — path may contain colons
        segments = self._urn.split(":", 5)
        if len(segments) != 6 or segments[0] != "urn":
            raise ValueError(f"Invalid URN format: {self._urn}")
        return segments[1], segments[2], segments[3], segments[4], segments[5]

    def __str__(self) -> str:
        return self._urn

    def __repr__(self) -> str:
        return f"URN({self._urn!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, URN):
            return self._urn == other._urn
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._urn)


@dataclass
class Node:
    """
    A discoverable resource in the security graph.

    Nodes are organized as a tree via parent_urn. A PostgreSQL column's
    full context is: database → schema → table → column. Nodes exist at
    every level of this hierarchy.

    The URN is the canonical identifier and dedup key — two ingestion runs
    that produce the same URN refer to the same node.
    """

    organization_id: uuid.UUID
    urn: URN
    parent_urn: URN | None = None
    metadata: NodeMetadata = field(default_factory=NodeMetadata)
    node_type: str = "unknown"

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset()
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset()


EDGE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "dsec:graph:edge")


@dataclass
class Edge:
    """
    A directed relationship between two nodes in the security graph.

    Edges are typed by RelationType which controls traversal semantics:
    - CONTAINS edges are traversed for structural/hierarchy queries.
    - HOSTS edges link infrastructure to the data resources they run.
    - DATA_TO_DATA edges are traversed for data flow queries.
    - CODE_TO_DATA edges are traversed for code impact analysis.
    - CODE_TO_CODE edges are traversed for call graph analysis.
    - PRINCIPAL_TO_DATA edges are traversed for permission queries.

    The metadata field carries provenance, confidence, evidence, and
    verification state.
    """

    uuid: uuid.UUID
    organization_id: uuid.UUID
    from_urn: URN
    to_urn: URN
    relation_type: RelationType
    metadata: EdgeMetadata = field(default_factory=EdgeMetadata)
    edge_type: str | None = None
