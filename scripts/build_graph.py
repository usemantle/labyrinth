"""
Build the full security graph and serialize it to JSON.

Scans the dsec database + the entire api_server codebase, stitches
CODE_TO_DATA edges, and serializes the graph to JSON.

Open scripts/visualize.html in a browser to view the graph
(serves graph_data.json via fetch).

Usage:
    PYTHONPATH=/Users/jacobburley/Desktop/data_security python scripts/build_graph.py
"""
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3

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
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader
from src.graph.loaders.codebase.plugins import Boto3S3Plugin, FastAPIPlugin, SQLAlchemyPlugin
from src.graph.loaders.object_store.s3.loader import S3BucketLoader
from src.graph.loaders.postgres.onprem_postgres_loader import OnPremPostgresLoader

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────

DB_RESOURCE = "postgresql://dsec_user:dsec_password@localhost:5434/dsec"
S3_BUCKET_ARN = "arn:aws:s3:::dsec-log-export-test"
S3_ACCOUNT_ID = "930900578103"
S3_REGION = "us-east-1"
CODE_PATHS = ["projects/api_server", "lib"]
EXCLUDE_DIRS = {
    "node_modules", "vendor", ".venv", "venv", "__pycache__", ".git",
    "dist", "build", "target", ".tox", ".mypy_cache", ".pytest_cache",
    ".ruff_cache", "egg-info",
    # Project-specific exclusions
    "migrations", "tests",
}
ORG_ID = uuid.uuid4()
OUTPUT_JSON = "graph_data.json"


# ── Node classification ──────────────────────────────────────────────

NK = NodeMetadataKey


def classify_node(node: Node) -> str:
    """Return a node type string based on metadata."""
    m = node.metadata
    if NK.FUNCTION_NAME in m:
        return "function"
    if NK.CLASS_NAME in m:
        return "class"
    if NK.COLUMN_NAME in m:
        return "column"
    if NK.TABLE_NAME in m:
        return "table"
    if NK.SCHEMA_NAME in m:
        return "schema"
    if NK.REPO_NAME in m:
        return "codebase"
    if NK.FILE_PATH in m and NK.CLASS_NAME not in m and NK.FUNCTION_NAME not in m:
        return "file"
    if NK.DATABASE_NAME in m:
        return "database"
    if NK.PATH_PATTERN in m:
        if NK.PARTITION_TYPE in m:
            return "s3_partition"
        if NK.OBJECT_COUNT not in m:
            return "s3_prefix"
        return "s3_object"
    if NK.BUCKET_NAME in m:
        return "s3_bucket"
    return "unknown"


# ── Serialization ────────────────────────────────────────────────────

def _serialize_node(node: Node) -> dict:
    return {
        "urn": str(node.urn),
        "organization_id": str(node.organization_id),
        "parent_urn": str(node.parent_urn) if node.parent_urn else None,
        "node_type": classify_node(node),
        "metadata": dict(node.metadata.items()),
    }


def _serialize_edge(edge) -> dict:
    return {
        "uuid": str(edge.uuid),
        "organization_id": str(edge.organization_id),
        "from_urn": str(edge.from_urn),
        "to_urn": str(edge.to_urn),
        "relation_type": edge.relation_type.value,
        "metadata": dict(edge.metadata.items()),
    }


def serialize_graph(nodes, edges, output_path: str):
    data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": [_serialize_node(n) for n in nodes],
        "edges": [_serialize_edge(e) for e in edges],
    }
    directory = Path(__file__).parent
    (directory / output_path).write_text(json.dumps(data, indent=2, default=str))
    logger.info("Graph JSON saved to %s", output_path)


# ── CODE_TO_DATA stitching ────────────────────────────────────────────


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

    # 1. Build table registry: table_name → table URN
    table_registry: dict[str, URN] = {}
    for node in data_nodes:
        if NodeMetadataKey.TABLE_NAME in node.metadata:
            table_registry[node.metadata[NodeMetadataKey.TABLE_NAME]] = node.urn

    if not table_registry:
        logger.warning("No table nodes found — skipping CODE_TO_DATA linking")
        return all_nodes, all_edges

    # 2. Build ORM registry: class_name → (class_urn, table_name)
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

    # 3. ORM class → table edges (confidence 1.0)
    class_edge_count = _create_orm_class_edges(
        organization_id, orm_registry, table_registry, all_edges,
    )
    logger.info("Created %d ORM class → table edges", class_edge_count)

    # 4. Function → table edges (confidence 0.9)
    func_edge_count = _create_function_edges(
        organization_id, code_nodes, orm_registry,
        table_registry, code_base_paths, all_edges,
    )
    logger.info("Created %d function → table edges", func_edge_count)

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


# ── Main ──────────────────────────────────────────────────────────────

def main():
    # 1. Scan database
    logger.info("Scanning database...")
    db_loader = OnPremPostgresLoader(organization_id=ORG_ID, resource=DB_RESOURCE)
    data_nodes, data_edges = db_loader.load(DB_RESOURCE)
    logger.info("Database: %d nodes, %d edges", len(data_nodes), len(data_edges))

    # 2. Scan codebase paths
    code_loader = FileSystemCodebaseLoader(
        organization_id=ORG_ID,
        plugins=[SQLAlchemyPlugin(), FastAPIPlugin(), Boto3S3Plugin()],
        exclude_dirs=EXCLUDE_DIRS,
    )
    code_nodes: list[Node] = []
    code_edges: list[Edge] = []
    for path in CODE_PATHS:
        logger.info("Scanning code at %s...", path)
        nodes, edges = code_loader.load(path)
        code_nodes.extend(nodes)
        code_edges.extend(edges)
        logger.info("  %s: %d nodes, %d edges", path, len(nodes), len(edges))

    # 3. Scan S3 bucket
    logger.info("Scanning S3 bucket %s...", S3_BUCKET_ARN)
    s3_client = boto3.client(
        "s3", region_name=S3_REGION,
    )
    s3_loader = S3BucketLoader(
        organization_id=ORG_ID,
        account_id=S3_ACCOUNT_ID,
        region=S3_REGION,
        s3_client=s3_client,
    )
    s3_nodes, s3_edges = s3_loader.load(S3_BUCKET_ARN)
    logger.info("S3: %d nodes, %d edges", len(s3_nodes), len(s3_edges))

    # 4. Stitch CODE_TO_DATA edges
    logger.info("Stitching code-to-data edges...")
    all_nodes, all_edges = stitch_code_to_data(
        ORG_ID, data_nodes, data_edges, code_nodes, code_edges,
        code_base_paths=CODE_PATHS,
    )

    # Merge S3 nodes/edges
    all_nodes.extend(s3_nodes)
    all_edges.extend(s3_edges)

    # Summarize
    by_type: dict[str, int] = {}
    for e in all_edges:
        by_type[e.relation_type.value] = by_type.get(e.relation_type.value, 0) + 1
    logger.info(
        "Combined: %d nodes, %d edges (%s)",
        len(all_nodes), len(all_edges),
        ", ".join(f"{k}={v}" for k, v in sorted(by_type.items())),
    )

    # 5. Serialize to JSON
    serialize_graph(all_nodes, all_edges, OUTPUT_JSON)

    # 6. Serve via HTTP so the browser can fetch graph_data.json
    import http.server
    import functools
    import webbrowser

    directory = Path(__file__).parent
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=str(directory),
    )
    server = http.server.HTTPServer(("localhost", 8787), handler)
    url = "http://localhost:8787/visualize.html"
    logger.info("Serving at %s  (Ctrl+C to stop)", url)
    webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Server stopped.")


if __name__ == "__main__":
    main()
