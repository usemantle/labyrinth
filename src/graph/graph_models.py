from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, field
from typing import Any, ClassVar


class NodeType(enum.StrEnum):
    """Valid node types in the security graph."""

    # Code
    CODEBASE = "codebase"
    FILE = "file"
    CLASS = "class"
    FUNCTION = "function"
    PACKAGE_MANIFEST = "package_manifest"
    DEPENDENCY = "dependency"

    # Data
    DATABASE = "database"
    SCHEMA = "schema"
    TABLE = "table"
    COLUMN = "column"
    RDS_CLUSTER = "rds_cluster"

    # AWS Infrastructure
    AWS_ACCOUNT = "aws_account"
    ECS_CLUSTER = "ecs_cluster"
    ECS_SERVICE = "ecs_service"
    ECS_TASK_DEFINITION = "ecs_task_definition"
    S3_BUCKET = "s3_bucket"
    S3_PREFIX = "s3_prefix"
    S3_PARTITION = "s3_partition"
    S3_OBJECT = "s3_object"
    IMAGE_REPOSITORY = "image_repository"
    IMAGE = "image"

    # Networking
    DNS_RECORD = "dns_record"
    LOAD_BALANCER = "load_balancer"
    BACKEND_GROUP = "backend_group"

    # Security
    SECURITY_GROUP = "security_group"
    NACL = "nacl"
    VPC = "vpc"

    # Identity
    IAM_ROLE = "iam_role"
    IAM_USER = "iam_user"
    IAM_POLICY = "iam_policy"
    IDENTITY = "identity"
    SSO_GROUP = "sso_group"
    SSO_USER = "sso_user"
    PERMISSION_SET = "aws:permission_set"

    # IdP (Okta, etc.)
    PERSON = "person"
    GROUP = "group"
    APPLICATION = "application"

    # Legacy / classifier-only
    DB_ROLE = "db_role"
    UNKNOWN = "unknown"


class EdgeType(enum.StrEnum):
    """Valid edge types in the security graph."""

    CONTAINS = "contains"
    HOSTS = "hosts"
    CALLS = "calls"
    INSTANTIATES = "instantiates"
    DEPENDS_ON = "depends_on"
    BUILDS = "builds"
    EXECUTES = "executes"
    READS = "reads"
    WRITES = "writes"
    MODELS = "models"
    REFERENCES = "references"
    SOFT_REFERENCE = "soft_reference"
    ALLOWS_TRAFFIC_TO = "allows_traffic_to"
    ASSUMES = "assumes"
    ATTACHES = "attaches"
    MEMBER_OF = "member_of"
    PROTECTED_BY = "protected_by"
    RESOLVES_TO = "resolves_to"
    ROUTES_TO = "routes_to"

    # Okta-sourced edges
    OKTA_ASSIGNED_TO = "okta:assigned_to"
    OKTA_MAPS_TO = "okta:maps_to"
    OKTA_PART_OF = "okta:part_of"
    OKTA_PUSHES_TO = "okta:pushes_to"

    # AWS Identity Center / SSO
    SSO_ASSIGNED_TO = "sso:assigned_to"

    UNKNOWN = "unknown"


def edge_namespace(edge_type: str) -> str | None:
    """Return the namespace prefix of an edge type (e.g. ``"idp"``), or ``None`` if unprefixed.

    Edge type strings may be namespaced as ``"<namespace>:<name>"`` (e.g. ``"okta:assigned_to"``).
    Unprefixed types (``"contains"``, ``"reads"``) belong to the default core namespace and
    return ``None``.
    """
    if not edge_type or ":" not in edge_type:
        return None
    return edge_type.split(":", 1)[0]


class NodeMetadataKey(enum.StrEnum):
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
    PACKAGE_MANAGER = "package_manager"
    MANIFEST_FILE = "manifest_file"
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

    # ── Container images ───────────────────────────────────────────────
    REPOSITORY_NAME = "repository_name"
    REPOSITORY_URI = "repository_uri"
    IMAGE_DIGEST = "image_digest"
    IMAGE_TAGS = "image_tags"
    IMAGE_PUSHED_AT = "image_pushed_at"
    IMAGE_SIZE_BYTES = "image_size_bytes"
    OCI_SOURCE = "oci_source"
    OCI_REVISION = "oci_revision"
    DOCKERFILE_BASE_IMAGES = "dockerfile_base_images"
    DOCKERFILE_ENTRYPOINT = "dockerfile_entrypoint"
    DOCKERFILE_CMD = "dockerfile_cmd"
    DOCKERFILE_WORKDIR = "dockerfile_workdir"
    DOCKERFILE_COPY_TARGETS = "dockerfile_copy_targets"

    # ── RDS ─────────────────────────────────────────────────────────────
    RDS_ENGINE = "rds_engine"
    RDS_ENDPOINT = "rds_endpoint"
    RDS_PORT = "rds_port"
    RDS_PUBLICLY_ACCESSIBLE = "rds_publicly_accessible"
    RDS_ENCRYPTION_ENABLED = "rds_encryption_enabled"
    RDS_STORAGE_ENCRYPTED = "rds_storage_encrypted"
    RDS_MULTI_AZ = "rds_multi_az"
    RDS_CLUSTER_ID = "rds_cluster_id"

    # ── ECS ─────────────────────────────────────────────────────────────
    ECS_CLUSTER_NAME = "ecs_cluster_name"
    ECS_SERVICE_NAME = "ecs_service_name"
    ECS_TASK_DEFINITION = "ecs_task_definition"
    ECS_TASK_FAMILY = "ecs_task_family"
    ECS_TASK_REVISION = "ecs_task_revision"
    ECS_CONTAINER_IMAGES = "ecs_container_images"
    ECS_TASK_ROLE_ARN = "ecs_task_role_arn"
    ECS_EXECUTION_ROLE_ARN = "ecs_execution_role_arn"

    # ── VPC / Security Groups / NACLs ───────────────────────────────────
    VPC_ID = "vpc_id"
    VPC_CIDR = "vpc_cidr"
    SG_ID = "sg_id"
    SG_NAME = "sg_name"
    SG_RULES_INGRESS = "sg_rules_ingress"
    SG_RULES_EGRESS = "sg_rules_egress"
    NACL_ID = "nacl_id"
    NACL_RULES = "nacl_rules"

    # ── IAM ─────────────────────────────────────────────────────────────
    IAM_TRUST_POLICY = "iam_trust_policy"
    IAM_USER_NAME = "iam_user_name"
    IAM_ACCESS_KEYS = "iam_access_keys"
    IAM_MFA_ENABLED = "iam_mfa_enabled"
    IAM_LAST_ACTIVITY = "iam_last_activity"
    IAM_POLICY_NAME = "iam_policy_name"
    IAM_POLICY_ARN = "iam_policy_arn"
    IAM_POLICY_DOCUMENT = "iam_policy_document"

    # ── SSO ─────────────────────────────────────────────────────────────
    SSO_GROUP_ID = "sso_group_id"
    SSO_GROUP_NAME = "sso_group_name"
    SSO_USER_ID = "sso_user_id"
    SSO_USER_NAME = "sso_user_name"
    SSO_USER_EMAIL = "sso_user_email"
    SSO_USER_EXTERNAL_ID = "sso_user_external_id"
    PERMISSION_SET_NAME = "permission_set_name"
    PERMISSION_SET_ARN = "permission_set_arn"
    PERMISSION_SET_INSTANCE_ARN = "permission_set_instance_arn"
    PERMISSION_SET_DESCRIPTION = "permission_set_description"
    PERMISSION_SET_SESSION_DURATION = "permission_set_session_duration"

    # ── IdP (Person / Group / Application) ──────────────────────────────
    PERSON_OKTA_ID = "person_okta_id"
    PERSON_EMAIL = "person_email"
    PERSON_LOGIN = "person_login"
    PERSON_STATUS = "person_status"
    PERSON_DISPLAY_NAME = "person_display_name"
    GROUP_OKTA_ID = "group_okta_id"
    GROUP_NAME = "group_name"
    GROUP_DESCRIPTION = "group_description"
    APP_OKTA_ID = "app_okta_id"
    APP_NAME = "app_name"
    APP_LABEL = "app_label"
    APP_SIGN_ON_MODE = "app_sign_on_mode"
    APP_STATUS = "app_status"

    # ── DNS ──────────────────────────────────────────────────────────────
    DNS_RECORD_NAME = "dns_record_name"
    DNS_RECORD_TYPE = "dns_record_type"
    DNS_ZONE_NAME = "dns_zone_name"
    DNS_ZONE_PRIVATE = "dns_zone_private"
    DNS_ZONE_ID = "dns_zone_id"
    DNS_TTL = "dns_ttl"
    DNS_VALUES = "dns_values"

    # ── Load balancer ────────────────────────────────────────────────────
    LB_TYPE = "lb_type"
    LB_SCHEME = "lb_scheme"
    LB_DNS_NAME = "lb_dns_name"
    LB_LISTENERS = "lb_listeners"
    LB_STATE = "lb_state"

    # ── Backend group ────────────────────────────────────────────────────
    BG_NAME = "bg_name"
    BG_PORT = "bg_port"
    BG_PROTOCOL = "bg_protocol"
    BG_TARGET_TYPE = "bg_target_type"
    BG_HEALTH_CHECK = "bg_health_check"
    BG_BACKEND_TYPE = "bg_backend_type"

    # ── API Gateway ──────────────────────────────────────────────────────
    API_GW_STAGE = "api_gw_stage"
    API_GW_ENDPOINT_TYPE = "api_gw_endpoint_type"
    API_GW_AUTH_TYPE = "api_gw_auth_type"
    API_GW_CUSTOM_DOMAINS = "api_gw_custom_domains"
    API_GW_INTEGRATION_URIS = "api_gw_integration_uris"

    # ── ECS networking ───────────────────────────────────────────────────
    ECS_TARGET_GROUP_ARNS = "ecs_target_group_arns"
    ECS_PUBLIC_IP = "ecs_public_ip"


class EdgeMetadataKey(enum.StrEnum):
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

    # ── AWS resource linking ──────────────────────────────────────────
    ASSUMED_VIA = "assumed_via"
    SG_RULE_PROTOCOL = "sg_rule_protocol"
    SG_RULE_PORT_RANGE = "sg_rule_port_range"
    SG_RULE_DIRECTION = "sg_rule_direction"

    # ── Networking ────────────────────────────────────────────────────
    LISTENER_PORT = "listener_port"
    LISTENER_PROTOCOL = "listener_protocol"

    # ── Stitcher provenance ──────────────────────────────────────────
    MATCH_KEY = "match_key"
    MATCH_VALUE = "match_value"

    # ── AWS Identity Center / SSO assignment ─────────────────────────
    ACCOUNT_ID = "account_id"
    PERMISSION_SET_ARN = "permission_set_arn"
    VIA_GROUP = "via_group"
    TRUST_POLICY_CONDITION = "trust_policy_condition"


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
    node_type: str = NodeType.UNKNOWN

    _allowed_outgoing_edges: ClassVar[frozenset[type]] = frozenset()
    _allowed_incoming_edges: ClassVar[frozenset[type]] = frozenset()


EDGE_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "dsec:graph:edge")


@dataclass
class Edge:
    """
    A directed relationship between two nodes in the security graph.

    The edge_type field carries all semantic meaning (e.g. "contains",
    "reads", "writes", "calls"). The metadata field carries provenance,
    confidence, evidence, and verification state.
    """

    uuid: uuid.UUID
    organization_id: uuid.UUID
    from_urn: URN
    to_urn: URN
    metadata: EdgeMetadata = field(default_factory=EdgeMetadata)
    edge_type: str = EdgeType.UNKNOWN


@dataclass
class Graph:
    """Container for a set of nodes and edges in the security graph."""

    nodes: list[Node] = field(default_factory=list)
    edges: list[Edge] = field(default_factory=list)

    def merge(self, other: Graph) -> None:
        """Append all nodes and edges from *other* into this graph."""
        self.nodes.extend(other.nodes)
        self.edges.extend(other.edges)

    def deduplicate_nodes(self) -> None:
        """Raise if any duplicate URNs exist — this indicates a configuration or logic error."""
        seen: set[str] = set()
        for node in self.nodes:
            urn_str = str(node.urn)
            if urn_str in seen:
                raise RuntimeError(
                    f"Duplicate node URN detected: {urn_str}. "
                    "This is symptomatic of a configuration or logical issue."
                )
            seen.add(urn_str)
