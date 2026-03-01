"""Code-to-data stitching for the security graph.

Creates CODE_TO_DATA edges between code nodes (ORM classes, functions)
and data nodes (database tables) by detecting ORM table mappings and
function-level references to ORM models.
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

from src.graph.graph_models import (
    Edge,
    EdgeMetadata,
    EdgeMetadataKey,
    Node,
    NodeMetadata,
    NodeMetadataKey,
    RelationType,
    URN,
)
from src.graph.loaders._helpers import make_edge

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
            all_edges.append(make_edge(
                organization_id,
                class_urn,
                table_urn,
                RelationType.CODE_TO_DATA,
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
                all_edges.append(make_edge(
                    organization_id,
                    node.urn,
                    table_urn,
                    RelationType.CODE_TO_DATA,
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
