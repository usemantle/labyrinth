"""Code-to-data stitching for the security graph.

Creates CODE_TO_DATA edges between code nodes (ORM classes, functions)
and data nodes (database tables) by detecting ORM table mappings and
function-level references to ORM models.

Also creates BuildsEdge links between Dockerfile FileNodes and
ImageRepositoryNodes using OCI label matching and name heuristics.
"""

from __future__ import annotations

import logging
import re
import uuid
from pathlib import Path

from src.graph.edges.builds_edge import BuildsEdge
from src.graph.edges.hosts_edge import HostsEdge
from src.graph.edges.models_edge import ModelsEdge
from src.graph.edges.reads_edge import ReadsEdge
from src.graph.edges.references_edge import ReferencesEdge
from src.graph.graph_models import (
    URN,
    Edge,
    EdgeMetadata,
    EdgeMetadataKey,
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

    logger.info("Created %d AWS cross-service stitching edges", edge_count)
    return all_nodes, all_edges
