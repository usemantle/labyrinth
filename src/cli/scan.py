"""Scan handler — orchestrates loader execution for registered targets."""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

from src.graph.enrichment.sensitivity_classifier import enrich_sensitivity
from src.graph.graph_models import URN, Edge, Node
from src.graph.loaders import LOADER_REGISTRY
from src.graph.loaders.loader import ConceptLoader
from src.graph.sinks.sink import Sink
from src.graph.stitching import stitch_code_to_data, stitch_code_to_images

logger = logging.getLogger(__name__)

# ── URN → loader dispatch ────────────────────────────────────────────

_URN_DISPATCH: dict[tuple[str, str], type[ConceptLoader]] = {}

for _loader_cls in LOADER_REGISTRY:
    # Build a dummy URN from each loader to extract its (provider, service) pair.
    _dummy_components = {c.name: c.default or "x" for c in _loader_cls.urn_components()}
    _dummy_urn = _loader_cls.build_target_urn(**_dummy_components)
    _URN_DISPATCH[(_dummy_urn.provider, _dummy_urn.service)] = _loader_cls


def _resolve_loader(urn: URN) -> type[ConceptLoader]:
    """Look up the loader class for a given target URN."""
    key = (urn.provider, urn.service)
    loader_cls = _URN_DISPATCH.get(key)
    if loader_cls is None:
        raise ValueError(
            f"No loader registered for URN scheme {key[0]}:{key[1]}. "
            f"Known schemes: {sorted(_URN_DISPATCH.keys())}"
        )
    return loader_cls


# ── Codebase target detection ────────────────────────────────────────

_CODEBASE_SERVICES = {"repo", "codebase"}


def _is_codebase_target(urn: URN) -> bool:
    return urn.service in _CODEBASE_SERVICES


# ── Scan orchestration ───────────────────────────────────────────────

def run_scan(
    project_name: str,
    project_id: uuid.UUID,
    targets: list[dict],
    sink: Sink,
    project_dir: Path,
) -> None:
    """Scan all given targets, stitch edges, and write results to the sink."""
    all_code_nodes: list[Node] = []
    all_code_edges: list[Edge] = []
    all_data_nodes: list[Node] = []
    all_data_edges: list[Edge] = []
    code_base_paths: list[str] = []

    for target in targets:
        urn = URN(target["urn"])
        credentials = target.get("credentials", {})
        loader_cls = _resolve_loader(urn)

        logger.info("Scanning %s with %s...", urn, loader_cls.display_name())

        kwargs: dict = {"project_dir": project_dir}

        # Instantiate plugins from config.
        plugin_names = target.get("plugins", [])
        if plugin_names:
            available = loader_cls.available_plugins()
            plugins = [available[n]() for n in plugin_names if n in available]
            if plugins:
                kwargs["plugins"] = plugins

        loader, resource = loader_cls.from_target_config(
            project_id, urn, credentials, **kwargs,
        )
        nodes, edges = loader.load(resource)
        logger.info(
            "  %s: %d nodes, %d edges", loader_cls.display_name(), len(nodes), len(edges),
        )

        if _is_codebase_target(urn):
            all_code_nodes.extend(nodes)
            all_code_edges.extend(edges)
            code_base_paths.append(resource)
        else:
            all_data_nodes.extend(nodes)
            all_data_edges.extend(edges)

    # Stitch code-to-data edges if both code and data nodes exist.
    if all_code_nodes and all_data_nodes:
        logger.info("Stitching code-to-data edges...")
        all_nodes, all_edges = stitch_code_to_data(
            project_id,
            all_data_nodes, all_data_edges,
            all_code_nodes, all_code_edges,
            code_base_paths,
        )
    else:
        all_nodes = all_data_nodes + all_code_nodes
        all_edges = list(all_data_edges) + list(all_code_edges)

    # Stitch code-to-image edges if image repositories exist.
    all_nodes, all_edges = stitch_code_to_images(
        project_id, all_nodes, all_edges,
    )

    # Enrich sensitivity metadata
    all_nodes = enrich_sensitivity(all_nodes)

    # Summarize
    by_type: dict[str, int] = {}
    for e in all_edges:
        by_type[e.edge_type] = by_type.get(e.edge_type, 0) + 1
    logger.info(
        "Combined: %d nodes, %d edges (%s)",
        len(all_nodes), len(all_edges),
        ", ".join(f"{k}={v}" for k, v in sorted(by_type.items())),
    )

    sink.write(all_nodes, all_edges)
