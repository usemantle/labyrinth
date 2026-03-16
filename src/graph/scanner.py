"""Scanner — orchestrates the ingest/stitch/resolve/post-process/validate/write pipeline."""

from __future__ import annotations

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.cli.settings import get_plugin_enable_mode
from src.graph.enrichment.sensitivity_classifier import enrich_sensitivity
from src.graph.graph_models import URN, Graph
from src.graph.loaders import LOADER_REGISTRY
from src.graph.loaders.loader import ConceptLoader
from src.graph.sinks.sink import Sink
from src.graph.stitchers import RESOLVER_REGISTRY, STITCHER_REGISTRY

logger = logging.getLogger(__name__)

# ── URN → loader dispatch ────────────────────────────────────────────

_URN_DISPATCH: dict[tuple[str, str], type[ConceptLoader]] = {}

for _loader_cls in LOADER_REGISTRY:
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


def _resolve_plugins(
    target: dict,
    loader_cls: type[ConceptLoader],
    urn: URN,
    global_config: dict,
) -> list:
    """Determine which plugins to use for a target."""
    available = loader_cls.available_plugins()
    if not available:
        return []

    explicit = target.get("plugins", [])
    if explicit:
        return [available[n]() for n in explicit if n in available]

    mode = get_plugin_enable_mode(global_config)
    if mode == "auto-enable-all-plugins":
        return [cls() for cls in available.values()]
    if mode == "auto-enable-relevant-plugins":
        root = Path(urn.path).expanduser().resolve()
        return [cls() for cls in available.values() if cls.auto_detect(root)]
    return []


class Scanner:
    """Orchestrates the full scan pipeline: ingest -> stitch -> resolve -> post-process -> validate -> write."""

    def __init__(
        self,
        project_name: str,
        project_id: uuid.UUID,
        targets: list[dict],
        sink: Sink,
        project_dir: Path,
        global_config: dict | None = None,
    ):
        self.project_name = project_name
        self.project_id = project_id
        self.targets = targets
        self.sink = sink
        self.project_dir = project_dir
        self.global_config = global_config or {}

    def run(self) -> None:
        graph, context = self._ingest()
        graph = self._stitch(graph, context)
        graph = self._resolve(graph, context)
        self._post_process(graph)
        self._validate(graph)
        self.sink.write(graph.nodes, graph.edges)

    def _ingest(self) -> tuple[Graph, dict]:
        """Load all targets in parallel (1 thread per target)."""
        graph = Graph()
        code_base_paths: list[str] = []

        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self._load_target, target)
                for target in self.targets
            ]

        for future in futures:
            target_graph, resource, is_code = future.result()
            graph.merge(target_graph)
            if is_code:
                code_base_paths.append(resource)

        context = {"code_base_paths": code_base_paths}
        return graph, context

    def _load_target(self, target: dict) -> tuple[Graph, str, bool]:
        """Load a single target. Returns (graph, resource_string, is_codebase)."""
        urn = URN(target["urn"])
        credentials = target.get("credentials", {})
        loader_cls = _resolve_loader(urn)

        logger.info("Scanning %s with %s...", urn, loader_cls.display_name())

        kwargs: dict = {"project_dir": self.project_dir}
        plugins = _resolve_plugins(target, loader_cls, urn, self.global_config)
        if plugins:
            kwargs["plugins"] = plugins

        loader, resource = loader_cls.from_target_config(
            self.project_id, urn, credentials, **kwargs,
        )
        nodes, edges = loader.load(resource)
        logger.info(
            "  %s: %d nodes, %d edges", loader_cls.display_name(), len(nodes), len(edges),
        )

        return Graph(nodes=nodes, edges=edges), resource, _is_codebase_target(urn)

    def _stitch(self, graph: Graph, context: dict) -> Graph:
        """Run all stitchers in parallel, merge new edges into the graph."""
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(stitcher_cls().stitch, self.project_id, graph, context)
                for stitcher_cls in STITCHER_REGISTRY
            ]

        for future in futures:
            result = future.result()
            graph.merge(result)

        # Log edge summary
        by_type: dict[str, int] = {}
        for e in graph.edges:
            by_type[e.edge_type] = by_type.get(e.edge_type, 0) + 1
        logger.info(
            "After stitching: %d nodes, %d edges (%s)",
            len(graph.nodes), len(graph.edges),
            ", ".join(f"{k}={v}" for k, v in sorted(by_type.items())),
        )

        return graph

    def _resolve(self, graph: Graph, context: dict) -> Graph:
        """Run resolvers sequentially (they mutate the graph)."""
        for resolver_cls in RESOLVER_REGISTRY:
            graph = resolver_cls().resolve(self.project_id, graph, context)
        return graph

    def _post_process(self, graph: Graph) -> None:
        """Enrich sensitivity metadata."""
        graph.nodes = enrich_sensitivity(graph.nodes)

    def _validate(self, graph: Graph) -> None:
        """Assert no duplicate URNs."""
        graph.deduplicate_nodes()
