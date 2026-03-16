"""Code-to-data stitching for the security graph.

Creates CODE_TO_DATA edges between code nodes (ORM classes, functions)
and data nodes (database tables) by detecting ORM table mappings and
function-level references to ORM models.

Also creates BuildsEdge links between Dockerfile FileNodes and
ImageRepositoryNodes using OCI label matching and name heuristics.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from pathlib import Path

from src.graph.edges.builds_edge import BuildsEdge
from src.graph.edges.executes_edge import ExecutesEdge
from src.graph.edges.hosts_edge import HostsEdge
from src.graph.edges.models_edge import ModelsEdge
from src.graph.edges.protected_by_edge import ProtectedByEdge
from src.graph.edges.reads_edge import ReadsEdge
from src.graph.edges.references_edge import ReferencesEdge
from src.graph.edges.resolves_to_edge import ResolvesToEdge
from src.graph.edges.routes_to_edge import RoutesToEdge
from src.graph.graph_models import (
    URN,
    Edge,
    EdgeMetadata,
    EdgeMetadataKey,
    EdgeType,
    Node,
    NodeMetadata,
    NodeMetadataKey,
    NodeType,
)

logger = logging.getLogger(__name__)


def stitch_code_to_data(
    organization_id: uuid.UUID,
    data_nodes: list[Node],
    data_edges: list[Edge],
    code_nodes: list[Node],
    code_edges: list[Edge],
    code_base_paths: list[str],
) -> tuple[list[Node], list[Edge]]:
    """Merge data + code graphs and create CODE_TO_DATA edges.

    Args:
        organization_id: Tenant identifier.
        data_nodes: Nodes from the database loader.
        data_edges: Edges from the database loader.
        code_nodes: Nodes from the codebase loader.
        code_edges: Edges from the codebase loader.
        code_base_paths: Root directory paths that were scanned
            (used to resolve relative file paths when reading
            function source text).

    Returns:
        Combined (nodes, edges) with CODE_TO_DATA edges added.
    """
    all_nodes = data_nodes + code_nodes
    all_edges = list(data_edges) + list(code_edges)

    # 1. Build table registry: table_name -> table URN
    table_registry: dict[str, URN] = {}
    for node in data_nodes:
        if NodeMetadataKey.TABLE_NAME in node.metadata:
            table_registry[node.metadata[NodeMetadataKey.TABLE_NAME]] = node.urn

    if not table_registry:
        logger.warning("No table nodes found — skipping CODE_TO_DATA linking")
        return all_nodes, all_edges

    # 2. Build ORM registry: class_name -> (class_urn, table_name)
    orm_registry: dict[str, tuple[URN, str]] = {}
    for node in code_nodes:
        if NodeMetadataKey.ORM_TABLE in node.metadata:
            orm_registry[node.metadata[NodeMetadataKey.CLASS_NAME]] = (
                node.urn,
                node.metadata[NodeMetadataKey.ORM_TABLE],
            )

    if not orm_registry:
        logger.warning("No ORM models found — skipping CODE_TO_DATA linking")
        return all_nodes, all_edges

    # 3. ORM class -> table edges (confidence 1.0)
    class_edge_count = _create_orm_class_edges(
        organization_id, orm_registry, table_registry, all_edges,
    )
    logger.info("Created %d ORM class -> table edges", class_edge_count)

    # 4. Function -> table edges (confidence 0.9)
    func_edge_count = _create_function_edges(
        organization_id, code_nodes, orm_registry,
        table_registry, code_base_paths, all_edges,
    )
    logger.info("Created %d function -> table edges", func_edge_count)

    return all_nodes, all_edges


def _create_orm_class_edges(
    organization_id: uuid.UUID,
    orm_registry: dict[str, tuple[URN, str]],
    table_registry: dict[str, URN],
    all_edges: list[Edge],
) -> int:
    count = 0
    for class_name, (class_urn, table_name) in orm_registry.items():
        if table_name in table_registry:
            table_urn = table_registry[table_name]
            all_edges.append(ModelsEdge.create(
                organization_id,
                class_urn,
                table_urn,
                metadata=EdgeMetadata({
                    EdgeMetadataKey.DETECTION_METHOD: "orm_tablename",
                    EdgeMetadataKey.CONFIDENCE: 1.0,
                    EdgeMetadataKey.ORM_FRAMEWORK: "sqlalchemy",
                    EdgeMetadataKey.ORM_CLASS: class_name,
                    EdgeMetadataKey.TABLE_NAME: table_name,
                }),
            ))
            count += 1
    return count


def _create_function_edges(
    organization_id: uuid.UUID,
    code_nodes: list[Node],
    orm_registry: dict[str, tuple[URN, str]],
    table_registry: dict[str, URN],
    code_base_paths: list[str],
    all_edges: list[Edge],
) -> int:
    count = 0
    for node in code_nodes:
        if NodeMetadataKey.FUNCTION_NAME not in node.metadata:
            continue

        source_text = _read_function_source(code_base_paths, node.metadata)
        if not source_text:
            continue

        for class_name, (_, table_name) in orm_registry.items():
            if class_name in source_text and table_name in table_registry:
                table_urn = table_registry[table_name]
                all_edges.append(ReadsEdge.create(
                    organization_id,
                    node.urn,
                    table_urn,
                    metadata=EdgeMetadata({
                        EdgeMetadataKey.DETECTION_METHOD: "orm_reference",
                        EdgeMetadataKey.CONFIDENCE: 0.9,
                        EdgeMetadataKey.REFERENCED_MODEL: class_name,
                        EdgeMetadataKey.TABLE_NAME: table_name,
                    }),
                ))
                count += 1
    return count


def _read_function_source(
    base_paths: list[str],
    metadata: NodeMetadata,
) -> str:
    """Read the source text of a function from disk."""
    rel_path = metadata.get(NodeMetadataKey.FILE_PATH, "")
    start = metadata.get(NodeMetadataKey.START_LINE)
    end = metadata.get(NodeMetadataKey.END_LINE)

    if not rel_path or start is None or end is None:
        return ""

    for base in base_paths:
        full_path = Path(base) / rel_path
        if full_path.is_file():
            try:
                lines = full_path.read_text(encoding="utf-8", errors="replace").splitlines()
                return "\n".join(lines[start:end + 1])
            except OSError:
                continue

    return ""


# ── Code-to-image stitching ──────────────────────────────────────────


def stitch_code_to_images(
    organization_id: uuid.UUID,
    all_nodes: list[Node],
    all_edges: list[Edge],
) -> tuple[list[Node], list[Edge]]:
    """Create BuildsEdge links between Dockerfile FileNodes and ImageRepositoryNodes.

    Matching strategies (in priority order):
    1. OCI label matching — ImageNode.oci_source matches CodebaseNode.repo_url/clone_url
    2. Name heuristic — repository name matches codebase name

    Args:
        organization_id: Tenant identifier.
        all_nodes: Combined node list (code + data).
        all_edges: Combined edge list (code + data).

    Returns:
        The same (nodes, edges) with BuildsEdge entries appended.
    """
    NK = NodeMetadataKey

    # Build registries
    image_repos: dict[str, URN] = {}  # repo_name -> repo URN
    image_oci_sources: dict[str, str] = {}  # repo_name -> oci_source URL
    codebase_by_url: dict[str, URN] = {}  # normalized URL -> codebase URN
    codebase_names: dict[str, URN] = {}  # codebase name -> codebase URN
    dockerfiles: list[Node] = []  # FileNodes with DOCKERFILE_BASE_IMAGES
    dockerfile_by_codebase: dict[str, list[Node]] = {}  # codebase URN str -> [FileNode]

    for node in all_nodes:
        if node.node_type == NodeType.IMAGE_REPOSITORY:
            repo_name = node.metadata.get(NK.REPOSITORY_NAME, "")
            if repo_name:
                image_repos[repo_name] = node.urn

        elif node.node_type == NodeType.IMAGE:
            oci_source = node.metadata.get(NK.OCI_SOURCE)
            if oci_source and node.parent_urn:
                # Find the repo name from parent
                for rname, rurn in image_repos.items():
                    if rurn == node.parent_urn:
                        image_oci_sources[rname] = oci_source
                        break

        elif node.node_type == NodeType.CODEBASE:
            repo_url = node.metadata.get(NK.REPO_URL, "")
            clone_url = node.metadata.get(NK.CLONE_URL, "")
            repo_name = node.metadata.get(NK.REPO_NAME, "")
            if repo_url:
                codebase_by_url[_normalize_url(repo_url)] = node.urn
            if clone_url:
                codebase_by_url[_normalize_url(clone_url)] = node.urn
            if repo_name:
                codebase_names[repo_name] = node.urn

        elif node.node_type == NodeType.FILE and NK.DOCKERFILE_BASE_IMAGES in node.metadata:
            dockerfiles.append(node)

    # Map Dockerfiles to their parent codebase
    for df_node in dockerfiles:
        parent = df_node.parent_urn
        if parent:
            dockerfile_by_codebase.setdefault(str(parent), []).append(df_node)

    if not image_repos or not dockerfiles:
        return all_nodes, all_edges

    linked_repos: set[str] = set()
    edge_count = 0

    # Strategy 1: OCI label matching (confidence 1.0)
    for repo_name, oci_source in image_oci_sources.items():
        normalized = _normalize_url(oci_source)
        codebase_urn = codebase_by_url.get(normalized)
        if codebase_urn is None:
            continue

        repo_urn = image_repos[repo_name]
        dfs = dockerfile_by_codebase.get(str(codebase_urn), [])
        for df_node in dfs:
            all_edges.append(BuildsEdge.create(
                organization_id,
                df_node.urn,
                repo_urn,
                metadata=EdgeMetadata({
                    EdgeMetadataKey.DETECTION_METHOD: "oci_label",
                    EdgeMetadataKey.CONFIDENCE: 1.0,
                }),
            ))
            edge_count += 1
        linked_repos.add(repo_name)

    # Strategy 2: Name heuristic (confidence 0.8)
    for repo_name, repo_urn in image_repos.items():
        if repo_name in linked_repos:
            continue

        # Try matching repo name to a codebase name
        base_name = repo_name.rsplit("/", 1)[-1]  # strip ECR prefix
        codebase_urn = codebase_names.get(base_name)
        if codebase_urn is None:
            # Try fuzzy: strip common suffixes
            stripped = re.sub(r"[-_](api|app|service|server|web)$", "", base_name)
            codebase_urn = codebase_names.get(stripped)

        if codebase_urn is None:
            continue

        dfs = dockerfile_by_codebase.get(str(codebase_urn), [])
        for df_node in dfs:
            all_edges.append(BuildsEdge.create(
                organization_id,
                df_node.urn,
                repo_urn,
                metadata=EdgeMetadata({
                    EdgeMetadataKey.DETECTION_METHOD: "name_heuristic",
                    EdgeMetadataKey.CONFIDENCE: 0.8,
                }),
            ))
            edge_count += 1

    logger.info("Created %d code -> image repository edges", edge_count)
    return all_nodes, all_edges


# ── Dockerfile entrypoint stitching ──────────────────────────────────


# Runners/interpreters to skip when extracting the file argument
_KNOWN_RUNNERS = frozenset({
    "python", "python3", "node", "uvicorn", "gunicorn",
    "java", "npm", "sh", "bash", "ruby", "perl", "php",
    "dotnet", "go", "run", "exec", "deno", "uv", "npx",
    "poetry", "pipenv", "conda",
})


def _parse_exec_form(raw: str) -> list[str]:
    """Parse a JSON exec-form instruction like '["python", "src/main.py"]'."""
    raw = raw.strip()
    if raw.startswith("["):
        try:
            parts = json.loads(raw)
            if isinstance(parts, list):
                return [str(p) for p in parts]
        except (json.JSONDecodeError, ValueError):
            pass
    return []


def _parse_shell_form(raw: str) -> list[str]:
    """Parse a shell-form instruction like 'python src/main.py'."""
    return raw.strip().split()


def _extract_target_file(raw_instruction: str) -> str | None:
    """Extract the target file path from an ENTRYPOINT or CMD instruction.

    Returns None if the target cannot be determined (variables, ambiguous).
    """
    parts = _parse_exec_form(raw_instruction)
    if not parts:
        parts = _parse_shell_form(raw_instruction)
    if not parts:
        return None

    # Skip known runners to find the actual file argument
    file_arg = None
    for part in parts:
        # Skip flags (start with -)
        if part.startswith("-"):
            continue
        # Skip known runners
        basename = part.rsplit("/", 1)[-1]  # handle /usr/bin/python
        if basename in _KNOWN_RUNNERS:
            continue
        file_arg = part
        break

    if not file_arg:
        return None

    # Skip if it contains variable substitution
    if "$" in file_arg or "%" in file_arg:
        return None

    # Handle Python module notation: app.main:app -> app/main.py, main:app -> main.py
    if ":" in file_arg and not file_arg.startswith("/"):
        module_part = file_arg.split(":")[0]
        if "/" not in module_part:
            file_arg = module_part.replace(".", "/") + ".py"

    return file_arg


def _resolve_container_path_to_codebase(
    file_arg: str,
    workdir: str | None,
    copy_targets: list[str],
) -> str:
    """Map a container-internal path back to a codebase-relative path.

    Uses WORKDIR and COPY destination mappings to strip container prefixes.
    """
    # Normalize: remove leading /
    path = file_arg.lstrip("/")

    # If WORKDIR is set and the path is relative, it's relative to WORKDIR
    # The common pattern: WORKDIR /app, COPY . ., CMD python src/main.py
    # In this case src/main.py is already codebase-relative
    if not file_arg.startswith("/"):
        # Relative path — likely already codebase-relative when COPY . . is used
        return path

    # Absolute path — try to strip WORKDIR prefix
    if workdir:
        wd = workdir.strip("/")
        if path.startswith(wd + "/"):
            return path[len(wd) + 1:]
        if path == wd:
            return ""

    # Try COPY destination mappings
    for dest in copy_targets:
        dest_norm = dest.strip("/")
        if dest_norm and path.startswith(dest_norm + "/"):
            return path[len(dest_norm) + 1:]

    return path


def stitch_dockerfile_entrypoints(
    organization_id: uuid.UUID,
    all_nodes: list[Node],
    all_edges: list[Edge],
) -> tuple[list[Node], list[Edge]]:
    """Create ExecutesEdge links between Dockerfile FileNodes and their entrypoint files.

    Parses ENTRYPOINT/CMD instructions to determine which code file a Dockerfile
    runs, then creates an ``executes`` edge to that file.

    Args:
        organization_id: Tenant identifier.
        all_nodes: Combined node list.
        all_edges: Combined edge list.

    Returns:
        The same (nodes, edges) with ExecutesEdge entries appended.
    """
    NK = NodeMetadataKey

    # Build registries
    dockerfiles: list[Node] = []
    # Map codebase URN -> {relative_path -> file URN}
    file_nodes_by_codebase: dict[str, dict[str, URN]] = {}
    # Map file URN -> parent codebase URN
    file_to_codebase: dict[str, str] = {}
    # Contains edges: parent -> child
    contains_children: dict[str, list[str]] = {}

    for edge in all_edges:
        if edge.edge_type == "contains":
            contains_children.setdefault(str(edge.from_urn), []).append(str(edge.to_urn))

    for node in all_nodes:
        if node.node_type == NodeType.FILE:
            has_entrypoint = NK.DOCKERFILE_ENTRYPOINT in node.metadata
            has_cmd = NK.DOCKERFILE_CMD in node.metadata
            if has_entrypoint or has_cmd:
                dockerfiles.append(node)

            # Index all files by their codebase for lookup
            if node.parent_urn and NK.FILE_PATH in node.metadata:
                codebase_key = str(node.parent_urn)
                file_nodes_by_codebase.setdefault(codebase_key, {})
                rel_path = node.metadata[NK.FILE_PATH]
                file_nodes_by_codebase[codebase_key][rel_path] = node.urn
                file_to_codebase[str(node.urn)] = codebase_key

    if not dockerfiles:
        return all_nodes, all_edges

    edge_count = 0

    for df_node in dockerfiles:
        # Determine entrypoint command (prefer ENTRYPOINT, fall back to CMD)
        raw = df_node.metadata.get(NK.DOCKERFILE_ENTRYPOINT)
        if not raw:
            raw = df_node.metadata.get(NK.DOCKERFILE_CMD)
        if not raw:
            continue

        target_file = _extract_target_file(raw)
        if not target_file:
            continue

        # Get WORKDIR and COPY context
        workdir = df_node.metadata.get(NK.DOCKERFILE_WORKDIR)
        copy_targets_raw = df_node.metadata.get(NK.DOCKERFILE_COPY_TARGETS, "")
        copy_targets = [t for t in copy_targets_raw.split(",") if t] if copy_targets_raw else []

        # Resolve container path to codebase-relative path
        resolved_path = _resolve_container_path_to_codebase(
            target_file, workdir, copy_targets,
        )

        if not resolved_path:
            continue

        # Find the parent codebase of this Dockerfile
        codebase_key = str(df_node.parent_urn) if df_node.parent_urn else None
        if not codebase_key:
            continue

        file_registry = file_nodes_by_codebase.get(codebase_key, {})
        target_urn = file_registry.get(resolved_path)

        if target_urn:
            all_edges.append(ExecutesEdge.create(
                organization_id,
                df_node.urn,
                target_urn,
                metadata=EdgeMetadata({
                    EdgeMetadataKey.DETECTION_METHOD: "static_parse",
                    EdgeMetadataKey.CONFIDENCE: 0.9,
                }),
            ))
            edge_count += 1

    logger.info("Created %d Dockerfile -> entrypoint executes edges", edge_count)
    return all_nodes, all_edges


def _normalize_url(url: str) -> str:
    """Normalize a git/repository URL for comparison."""
    url = url.rstrip("/")
    url = re.sub(r"\.git$", "", url)
    url = re.sub(r"^https?://", "", url)
    url = re.sub(r"^git@([^:]+):", r"\1/", url)
    return url.lower()


# ── AWS resource stitching ──────────────────────────────────────────


def stitch_aws_resources(
    organization_id: uuid.UUID,
    all_nodes: list[Node],
    all_edges: list[Edge],
) -> tuple[list[Node], list[Edge]]:
    """Create cross-service edges between AWS resources and existing graph nodes.

    Stitching rules:
    1. RDS -> Database: Match RDS endpoint to DatabaseNode host -> HostsEdge
    2. ECS Task -> ECR Image: Match container image URIs to ImageRepositoryNode -> ReferencesEdge

    Args:
        organization_id: Tenant identifier.
        all_nodes: Combined node list.
        all_edges: Combined edge list.

    Returns:
        The same (nodes, edges) with cross-service edges appended.
    """
    NK = NodeMetadataKey

    # Build registries
    rds_by_endpoint: dict[str, URN] = {}  # endpoint -> rds URN
    databases_by_host: dict[str, URN] = {}  # host -> database URN
    ecr_by_uri_prefix: dict[str, URN] = {}  # repo_uri -> ecr URN
    task_defs: list[Node] = []

    for node in all_nodes:
        if node.node_type == NodeType.RDS_CLUSTER:
            endpoint = node.metadata.get(NK.RDS_ENDPOINT)
            if endpoint:
                rds_by_endpoint[endpoint] = node.urn

        elif node.node_type == NodeType.DATABASE:
            host = node.metadata.get(NK.HOST)
            if host:
                databases_by_host[host] = node.urn

        elif node.node_type == NodeType.IMAGE_REPOSITORY:
            repo_uri = node.metadata.get(NK.REPOSITORY_URI)
            if repo_uri:
                ecr_by_uri_prefix[repo_uri] = node.urn

        elif node.node_type == NodeType.ECS_TASK_DEFINITION:
            task_defs.append(node)

    edge_count = 0

    # 1. RDS -> Database: match endpoint to host
    for endpoint, rds_urn in rds_by_endpoint.items():
        db_urn = databases_by_host.get(endpoint)
        if db_urn:
            all_edges.append(HostsEdge.create(
                organization_id, rds_urn, db_urn,
                metadata=EdgeMetadata({
                    EdgeMetadataKey.DETECTION_METHOD: "endpoint_match",
                    EdgeMetadataKey.CONFIDENCE: 1.0,
                }),
            ))
            edge_count += 1

    # 2. ECS Task -> ECR Image: match container image URIs
    for td_node in task_defs:
        images = td_node.metadata.get(NK.ECS_CONTAINER_IMAGES, [])
        if not isinstance(images, list):
            continue
        for image_uri in images:
            # Image URI format: {account}.dkr.ecr.{region}.amazonaws.com/{repo}:{tag}
            # Strip tag/digest to get the repo URI
            repo_uri = image_uri.split(":")[0] if ":" in image_uri else image_uri
            repo_uri = repo_uri.split("@")[0] if "@" in repo_uri else repo_uri

            ecr_urn = ecr_by_uri_prefix.get(repo_uri)
            if ecr_urn:
                all_edges.append(ReferencesEdge.create(
                    organization_id, td_node.urn, ecr_urn,
                    metadata=EdgeMetadata({
                        EdgeMetadataKey.DETECTION_METHOD: "image_uri_match",
                        EdgeMetadataKey.CONFIDENCE: 1.0,
                    }),
                ))
                edge_count += 1

    # 3. Resolve "unknown" VPC security group URNs in ProtectedByEdge edges.
    #    ECS and ELBv2 plugins don't know the VPC ID when creating edges, so
    #    they use "unknown" as a placeholder.  Replace with the real SG URN.
    sg_by_id: dict[str, URN] = {}  # sg_id -> real URN
    for node in all_nodes:
        if node.node_type == NodeType.SECURITY_GROUP:
            sg_id = node.metadata.get(NK.SG_ID, "")
            if sg_id:
                sg_by_id[sg_id] = node.urn
    if sg_by_id:
        resolved_count = 0
        new_edges: list[Edge] = []
        drop_indices: list[int] = []
        for i, edge in enumerate(all_edges):
            if edge.edge_type != EdgeType.PROTECTED_BY:
                continue
            to_str = str(edge.to_urn)
            if ":unknown/sg/" not in to_str:
                continue
            # Extract sg_id from urn:aws:vpc:{acct}:{region}:unknown/sg/{sg_id}
            sg_id = to_str.rsplit("/sg/", 1)[-1]
            real_urn = sg_by_id.get(sg_id)
            if real_urn:
                new_edges.append(ProtectedByEdge.create(
                    organization_id, edge.from_urn, real_urn,
                    metadata=edge.metadata,
                ))
                drop_indices.append(i)
                resolved_count += 1
        # Remove old edges (iterate in reverse to preserve indices)
        for i in reversed(drop_indices):
            all_edges.pop(i)
        all_edges.extend(new_edges)
        if resolved_count:
            logger.info("Resolved %d security group URNs from 'unknown' placeholders", resolved_count)

    logger.info("Created %d AWS cross-service stitching edges", edge_count)
    return all_nodes, all_edges


# ── Networking stitching ─────────────────────────────────────────────


def _normalize_lb_dns(dns_name: str) -> str:
    """Normalize a load balancer DNS name for comparison.

    Strips scheme prefixes (https://) and trailing dots/slashes.
    """
    name = dns_name.lower().strip()
    # Strip scheme prefix (API Gateway stores https://...)
    name = re.sub(r"^https?://", "", name)
    name = name.rstrip("./")
    return name


def stitch_networking(
    organization_id: uuid.UUID,
    all_nodes: list[Node],
    all_edges: list[Edge],
) -> tuple[list[Node], list[Edge]]:
    """Create cross-service edges for the networking topology.

    Stitching rules:
    1. DNS -> LB: Match DNS alias/CNAME values to LoadBalancerNode lb_dns_name
       (also matches API Gateway custom domain DNS names)
    2. API GW -> ALB: Match API Gateway integration URIs (listener ARNs) to
       ALB listener ARNs

    Args:
        organization_id: Tenant identifier.
        all_nodes: Combined node list.
        all_edges: Combined edge list.

    Returns:
        The same (nodes, edges) with networking edges appended.
    """
    NK = NodeMetadataKey

    # Build registries
    lb_by_dns: dict[str, URN] = {}  # normalized dns_name -> LB URN
    dns_records: list[Node] = []
    bg_by_arn: dict[str, URN] = {}  # target group ARN -> BG URN
    ecs_services: list[Node] = []
    api_gw_nodes: list[Node] = []  # API Gateway LB nodes with integrations
    lb_by_listener_arn: dict[str, URN] = {}  # listener ARN -> LB URN

    for node in all_nodes:
        if node.node_type == NodeType.LOAD_BALANCER:
            dns_name = node.metadata.get(NK.LB_DNS_NAME, "")
            if dns_name:
                lb_by_dns[_normalize_lb_dns(dns_name)] = node.urn

            # Also index API Gateway custom domain DNS names
            custom_domains = node.metadata.get(NK.API_GW_CUSTOM_DOMAINS, [])
            if isinstance(custom_domains, list):
                for cd in custom_domains:
                    lb_by_dns[_normalize_lb_dns(cd)] = node.urn

            # Track API GW nodes with integration URIs
            if node.metadata.get(NK.API_GW_INTEGRATION_URIS):
                api_gw_nodes.append(node)

            # Index LB listeners by ARN for API GW -> ALB matching
            listeners = node.metadata.get(NK.LB_LISTENERS, [])
            lb_arn = node.metadata.get(NK.ARN, "")
            if isinstance(listeners, list) and lb_arn:
                # Build listener ARNs from LB ARN pattern
                # Listener ARNs follow: {lb_arn_base}/listener/{id}
                # We can't know exact listener ARN from LB data alone,
                # so we index the LB ARN prefix for prefix matching
                lb_by_listener_arn[lb_arn] = node.urn

        elif node.node_type == NodeType.DNS_RECORD:
            dns_records.append(node)

        elif node.node_type == NodeType.BACKEND_GROUP:
            arn = node.metadata.get(NK.ARN, "")
            if arn:
                bg_by_arn[arn] = node.urn

        elif node.node_type == NodeType.ECS_SERVICE:
            ecs_services.append(node)

    edge_count = 0

    # 1. DNS -> LB: match alias/CNAME values to LB DNS names
    for dns_node in dns_records:
        values = dns_node.metadata.get(NK.DNS_VALUES, [])
        if not isinstance(values, list):
            continue
        for value in values:
            normalized = _normalize_lb_dns(value)
            lb_urn = lb_by_dns.get(normalized)
            if lb_urn:
                all_edges.append(ResolvesToEdge.create(
                    organization_id, dns_node.urn, lb_urn,
                    metadata=EdgeMetadata({
                        EdgeMetadataKey.DETECTION_METHOD: "dns_alias_match",
                        EdgeMetadataKey.CONFIDENCE: 1.0,
                    }),
                ))
                edge_count += 1

    # 2. API GW -> ALB: match integration URIs (listener ARNs) to LB ARNs
    # LB ARN:       arn:aws:elasticloadbalancing:...:loadbalancer/app/lb-name/id
    # Listener ARN: arn:aws:elasticloadbalancing:...:listener/app/lb-name/id/listener-id
    # We extract the LB path (app/lb-name/id) and check if the integration
    # URI contains it as a listener ARN.
    for apigw_node in api_gw_nodes:
        integration_uris = apigw_node.metadata.get(NK.API_GW_INTEGRATION_URIS, [])
        if not isinstance(integration_uris, list):
            continue
        for uri in integration_uris:
            for lb_arn, lb_urn in lb_by_listener_arn.items():
                # Extract the path after "loadbalancer/" in the LB ARN
                lb_marker = "loadbalancer/"
                lb_idx = lb_arn.find(lb_marker)
                if lb_idx < 0:
                    continue
                lb_path = lb_arn[lb_idx + len(lb_marker):]
                # Check if integration URI is a listener for this LB
                if f"listener/{lb_path}" in uri:
                    all_edges.append(RoutesToEdge.create(
                        organization_id, apigw_node.urn, lb_urn,
                        metadata=EdgeMetadata({
                            EdgeMetadataKey.DETECTION_METHOD: "apigw_integration_match",
                            EdgeMetadataKey.CONFIDENCE: 1.0,
                        }),
                    ))
                    edge_count += 1
                    break

    # 3. BG -> ECS: match target group ARNs from ECS service loadBalancers config
    for svc_node in ecs_services:
        tg_arns = svc_node.metadata.get(NK.ECS_TARGET_GROUP_ARNS, [])
        if not isinstance(tg_arns, list):
            continue
        for tg_arn in tg_arns:
            bg_urn = bg_by_arn.get(tg_arn)
            if bg_urn:
                all_edges.append(RoutesToEdge.create(
                    organization_id, bg_urn, svc_node.urn,
                    metadata=EdgeMetadata({
                        EdgeMetadataKey.DETECTION_METHOD: "ecs_target_group_match",
                        EdgeMetadataKey.CONFIDENCE: 1.0,
                    }),
                ))
                edge_count += 1

    logger.info("Created %d networking stitching edges", edge_count)
    return all_nodes, all_edges
