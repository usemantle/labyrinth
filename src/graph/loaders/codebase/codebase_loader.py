"""
Abstract codebase loader for the security graph.

Discovers files, classes, and functions/methods within a local directory
and transforms them into graph Nodes and Edges. Concrete subclasses
provide URN construction via build_urn().

AST analysis uses ast-grep-py for multi-language structural extraction.
"""

from __future__ import annotations

import abc
import logging
import os
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

from ast_grep_py import SgRoot

from src.graph.edges.contains_edge import ContainsEdge
from src.graph.graph_models import (
    URN,
    Edge,
    Node,
    NodeMetadataKey,
)
from src.graph.loaders.codebase.plugins._base import CodebasePlugin
from src.graph.loaders.codebase.resolvers import LANGUAGE_ANALYZERS
from src.graph.loaders.loader import ConceptLoader
from src.graph.nodes.class_node import ClassNode
from src.graph.nodes.codebase_node import CodebaseNode
from src.graph.nodes.file_node import FileNode
from src.graph.nodes.function_node import FunctionNode

logger = logging.getLogger(__name__)


# ── Post-processing context ─────────────────────────────────────────


@dataclass
class PostProcessContext:
    """State accumulated during scanning, available for post-processing.

    This is a core loader concept — resolvers and plugins both receive it.
    """

    root_path: Path
    root_name: str
    organization_id: uuid.UUID
    file_sources: dict[str, str]
    """Mapping of rel_path → source text for every scanned file."""
    file_languages: dict[str, str]
    """Mapping of rel_path → ast-grep language name."""
    build_urn: Callable[..., URN]
    """The loader's build_urn method for constructing URNs."""

@dataclass
class _FileResult:
    """Result of processing a single file in Phase 1."""

    rel_path: str
    language: str | None
    nodes: list[Node]
    edges: list[Edge]
    source: str | None


# ── File extension → ast-grep language mapping ───────────────────────

EXTENSION_TO_LANGUAGE: dict[str, str] = {
    ".py": "python",
    ".js": "javascript",
    ".ts": "typescript",
    ".tsx": "tsx",
    ".jsx": "javascript",
    ".java": "java",
    ".rb": "ruby",
    ".rs": "rust",
    ".kt": "kotlin",
    ".scala": "scala",
    ".cs": "c_sharp",
    ".c": "c",
    ".cpp": "cpp",
    ".h": "c",
    ".hpp": "cpp",
    ".swift": "swift",
    ".sql": "sql",
}

# ── Directories to always skip ───────────────────────────────────────

DEFAULT_EXCLUDE_DIRS: set[str] = {
    "node_modules",
    "vendor",
    ".venv",
    "venv",
    "__pycache__",
    ".git",
    "dist",
    "build",
    "target",
    ".tox",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "egg-info",
}


# ── Per-language AST extraction config ───────────────────────────────


@dataclass
class LanguageConfig:
    """How to extract classes and functions from a given language's AST."""

    class_kinds: list[str]
    function_kinds: list[str]
    # AST kinds that wrap definitions (e.g. decorated_definition in Python).
    # Maps wrapper kind → field name containing the inner definition.
    wrapper_kinds: dict[str, str] = field(default_factory=dict)
    # AST kinds that are recursed into without creating a node.
    # Maps kind → body field name (e.g. impl_item → "body" in Rust).
    transparent_kinds: dict[str, str] = field(default_factory=dict)


LANGUAGE_CONFIGS: dict[str, LanguageConfig] = {
    "python": LanguageConfig(
        class_kinds=["class_definition"],
        function_kinds=["function_definition"],
        wrapper_kinds={"decorated_definition": "definition"},
    ),
    "javascript": LanguageConfig(
        class_kinds=["class_declaration"],
        function_kinds=["function_declaration", "method_definition"],
    ),
    "typescript": LanguageConfig(
        class_kinds=["class_declaration"],
        function_kinds=["function_declaration", "method_definition"],
    ),
    "tsx": LanguageConfig(
        class_kinds=["class_declaration"],
        function_kinds=["function_declaration", "method_definition"],
    ),
    "java": LanguageConfig(
        class_kinds=["class_declaration", "interface_declaration"],
        function_kinds=["method_declaration"],
    ),
    "rust": LanguageConfig(
        class_kinds=["struct_item", "enum_item", "trait_item"],
        function_kinds=["function_item"],
        transparent_kinds={"impl_item": "body"},
    ),
}


class CodebaseLoader(ConceptLoader, abc.ABC):
    """Abstract codebase loader.

    Implements ``load()`` using ast-grep to discover files, classes,
    and functions/methods and produce graph Nodes and Edges.  The
    ``build_urn()`` method remains abstract so concrete subclasses can
    define provider-specific URN schemes (GitHub vs on-prem vs Bitbucket).
    """

    def __init__(
        self,
        organization_id,
        *,
        exclude_dirs: set[str] | None = None,
        plugins: list[CodebasePlugin] | None = None,
        max_workers: int | None = None,
    ):
        super().__init__(organization_id)
        self._exclude_dirs = exclude_dirs or DEFAULT_EXCLUDE_DIRS
        self._plugins = plugins or []
        self._max_workers = max_workers

    @classmethod
    def available_plugins(cls) -> dict[str, type[CodebasePlugin]]:
        from src.graph.loaders.codebase.plugins import (
            Boto3S3Plugin,
            FastAPIPlugin,
            FlaskPlugin,
            RequestsPlugin,
            SQLAlchemyPlugin,
            UvPlugin,
        )
        from src.graph.loaders.codebase.plugins.python_dependency_linker import (
            PythonDependencyLinkerPlugin,
        )
        return {
            "sqlalchemy": SQLAlchemyPlugin,
            "fastapi": FastAPIPlugin,
            "flask": FlaskPlugin,
            "requests": RequestsPlugin,
            "boto3-s3": Boto3S3Plugin,
            "uv": UvPlugin,
            "python-imports": PythonDependencyLinkerPlugin,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, resource: str) -> tuple[list[Node], list[Edge]]:
        """Discover nodes and edges from a local directory.

        Args:
            resource: Path to the local directory to scan.

        Returns:
            A tuple of (nodes, edges) discovered from the codebase.
        """

        root_path = Path(resource).expanduser()
        root_name = self._get_root_name(resource)
        files = self._enumerate_files(root_path)
        nodes: list[Node] = []
        edges: list[Edge] = []
        file_sources: dict[str, str] = {}
        file_languages: dict[str, str] = {}

        # Codebase root node
        codebase_urn = self.build_urn(root_name)
        nodes.append(self._build_codebase_node(codebase_urn, root_name, len(files)))

        # Phase 1: Structural extraction (parallelized per file)
        workers = self._max_workers or min(os.cpu_count() or 4, 8, len(files) or 1)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    self._process_single_file,
                    file_path, root_path, root_name, codebase_urn,
                ): file_path
                for file_path in files
            }
            for future in as_completed(futures):
                result = future.result()
                nodes.extend(result.nodes)
                edges.extend(result.edges)
                if result.source is not None and result.language:
                    file_sources[result.rel_path] = result.source
                    file_languages[result.rel_path] = result.language

        logger.info(
            "Structural extraction: %d nodes, %d edges from %s",
            len(nodes), len(edges), root_name,
        )

        # Phase 2: Post-processing (resolvers + plugins)
        ctx = PostProcessContext(
            root_path=root_path.resolve(),
            root_name=root_name,
            organization_id=self.organization_id,
            file_sources=file_sources,
            file_languages=file_languages,
            build_urn=self.build_urn,
        )

        # 2a. Language analysis (import resolution, call graph) and
        #     plugin post-processing, grouped by detected language.
        nodes, edges = self._run_language_analysis(nodes, edges, ctx)

        logger.info(
            "After post-processing: %d nodes, %d edges from %s",
            len(nodes), len(edges), root_name,
        )

        return nodes, edges

    # ------------------------------------------------------------------
    # Per-file processing (called from thread pool)
    # ------------------------------------------------------------------

    def _process_single_file(
        self,
        file_path: Path,
        root_path: Path,
        root_name: str,
        codebase_urn: URN,
    ) -> _FileResult:
        """Process one file independently, returning all nodes and edges."""
        rel_path = str(file_path.relative_to(root_path))
        language = self._detect_language(file_path)

        file_urn = self.build_urn(root_name, rel_path)
        file_node = FileNode.create(
            self.organization_id,
            file_urn,
            codebase_urn,
            file_path=rel_path,
            language=language or "unknown",
            size_bytes=file_path.stat().st_size,
        )
        contains_edge = ContainsEdge.create(
            self.organization_id, codebase_urn, file_urn,
        )

        result_nodes: list[Node] = [file_node]
        result_edges: list[Edge] = [contains_edge]
        source: str | None = None

        if language and language in LANGUAGE_CONFIGS:
            file_nodes, file_edges, source = self._analyze_file(
                file_path, language, file_urn, root_name, rel_path,
            )
            result_nodes.extend(file_nodes)
            result_edges.extend(file_edges)

        return _FileResult(
            rel_path=rel_path,
            language=language,
            nodes=result_nodes,
            edges=result_edges,
            source=source,
        )

    # ------------------------------------------------------------------
    # Overridable hooks
    # ------------------------------------------------------------------

    def _get_root_name(self, resource: str) -> str:
        """Return the name used as the first URN path segment.

        Override in subclasses that know the canonical name (e.g. GitHub
        repo name) rather than relying on the local directory name.
        """
        return Path(resource).name

    def _build_codebase_node(
        self,
        codebase_urn: URN,
        root_name: str,
        file_count: int,
    ) -> Node:
        """Build the codebase root node.

        Override in subclasses to add provider-specific metadata
        (e.g. scanned_commit, default_branch, visibility).
        """
        return CodebaseNode.create(
            self.organization_id,
            codebase_urn,
            repo_name=root_name,
            file_count=file_count,
        )

    # ------------------------------------------------------------------
    # Post-processing: language analysis
    # ------------------------------------------------------------------

    def _run_language_analysis(
        self,
        nodes: list[Node],
        edges: list[Edge],
        ctx: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        """Run language analyzers and plugin post-processing.

        Groups files by detected language, runs the LanguageAnalyzer for
        each, then runs plugin ``post_process`` hooks — language-specific
        plugins for each detected language first, then language-agnostic
        plugins (those returning ``None`` from ``supported_languages``).
        """

        # Group files by language
        detected_languages: set[str] = set()
        files_by_lang: dict[str, dict[str, str]] = {}
        for rel_path, lang in ctx.file_languages.items():
            detected_languages.add(lang)
            files_by_lang.setdefault(lang, {})[rel_path] = ctx.file_sources[rel_path]

        # Run language analyzers (import resolution, call graph)
        for lang, sources in files_by_lang.items():
            analyzer = LANGUAGE_ANALYZERS.get(lang)
            if not analyzer:
                continue
            nodes, edges = analyzer.analyze(nodes, edges, sources, ctx)

        # Partition plugins by language affinity
        lang_plugins: list[CodebasePlugin] = []
        universal_plugins: list[CodebasePlugin] = []
        for plugin in self._plugins:
            langs = plugin.supported_languages()
            if langs is None:
                universal_plugins.append(plugin)
            elif langs & detected_languages:
                lang_plugins.append(plugin)

        # Run language-specific plugins, then universal plugins
        for plugin in lang_plugins + universal_plugins:
            nodes, edges = plugin.post_process(nodes, edges, ctx)

        return nodes, edges

    # ------------------------------------------------------------------
    # File enumeration
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def _enumerate_files(self, root_path: Path) -> list[Path]:
        """Return parseable files under root_path.

        Subclasses implement their own file discovery strategy.
        Returned paths must be absolute with suffixes in EXTENSION_TO_LANGUAGE.
        """
        ...

    # ------------------------------------------------------------------
    # Language detection
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_language(file_path: Path) -> str | None:
        """Map file extension to ast-grep language name."""
        return EXTENSION_TO_LANGUAGE.get(file_path.suffix)

    # ------------------------------------------------------------------
    # AST analysis
    # ------------------------------------------------------------------

    def _analyze_file(
        self,
        file_path: Path,
        language: str,
        file_urn: URN,
        root_name: str,
        rel_path: str,
    ) -> tuple[list[Node], list[Edge], str | None]:
        """Parse a file and extract class/function nodes with CONTAINS edges.

        Returns:
            (nodes, edges, source_text) — source_text is None if unreadable.
        """
        try:
            source = file_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            logger.warning("Could not read %s, skipping AST analysis", file_path)
            return [], [], None

        if not source.strip():
            return [], [], source

        try:
            root = SgRoot(source, language)
            ast_root = root.root()
        except Exception:
            logger.warning("Could not parse %s as %s", file_path, language)
            return [], [], source

        config = LANGUAGE_CONFIGS[language]

        nodes, edges = self._extract_from_scope(
            ast_root, config, file_urn, root_name, rel_path, [], language,
        )
        return nodes, edges, source

    def _extract_from_scope(
        self,
        scope_node,
        config: LanguageConfig,
        parent_urn: URN,
        root_name: str,
        rel_path: str,
        name_chain: list[str],
        language: str = "",
    ) -> tuple[list[Node], list[Edge]]:
        """Recursively extract classes and functions from an AST scope.

        Args:
            scope_node: The AST node representing the current scope
                        (module, class body, etc.).
            config: Language-specific extraction config.
            parent_urn: URN of the parent node (file or class).
            root_name: Repository/codebase root name.
            rel_path: Relative file path from repo root.
            name_chain: Stack of enclosing class/function names for
                        nested definitions.
            language: The ast-grep language name (e.g. "python").
        """
        nodes: list[Node] = []
        edges: list[Edge] = []

        # Track names for ordinal-based disambiguation of overloads
        name_counts: dict[str, int] = {}

        for child in scope_node.children():
            # Unwrap decorator/annotation wrappers
            actual = self._unwrap_node(child, config)
            kind = actual.kind()

            if kind in config.class_kinds:
                name_node = actual.field("name")
                if not name_node:
                    continue
                class_name = name_node.text()

                urn_segments = [root_name, rel_path] + name_chain + [class_name]
                class_urn = self.build_urn(*urn_segments)

                r = actual.range()
                base_classes = self._extract_base_classes(actual)

                class_node = ClassNode.create(
                    self.organization_id,
                    class_urn,
                    parent_urn,
                    class_name=class_name,
                    start_line=r.start.line,
                    end_line=r.end.line,
                )
                class_node.metadata[NodeMetadataKey.FILE_PATH] = rel_path
                if base_classes:
                    class_node.metadata[NodeMetadataKey.BASE_CLASSES] = base_classes

                # Run plugins on class node
                body = actual.field("body")
                body_source = body.text() if body else ""
                for plugin in self._plugins:
                    if language in plugin.supported_languages():
                        class_node = plugin.on_class_node(class_node, body_source)

                nodes.append(class_node)
                edges.append(ContainsEdge.create(
                    self.organization_id, parent_urn, class_urn,
                ))

                # Recurse into class body
                if body:
                    sub_nodes, sub_edges = self._extract_from_scope(
                        body, config, class_urn, root_name, rel_path,
                        name_chain + [class_name], language,
                    )
                    nodes.extend(sub_nodes)
                    edges.extend(sub_edges)

            elif kind in config.function_kinds:
                name_node = actual.field("name")
                if not name_node:
                    continue
                func_name = name_node.text()

                # Disambiguate overloaded names
                display_name = self._disambiguate_name(func_name, name_counts)

                urn_segments = [root_name, rel_path] + name_chain + [display_name]
                func_urn = self.build_urn(*urn_segments)

                r = actual.range()
                is_method = len(name_chain) > 0

                func_node = FunctionNode.create(
                    self.organization_id,
                    func_urn,
                    parent_urn,
                    function_name=func_name,
                    start_line=r.start.line,
                    end_line=r.end.line,
                    is_method=is_method,
                )
                func_node.metadata[NodeMetadataKey.FILE_PATH] = rel_path

                # Run plugins on function node (child.text() includes decorators)
                func_source = child.text()
                for plugin in self._plugins:
                    if language in plugin.supported_languages():
                        func_node = plugin.on_function_node(func_node, func_source)

                nodes.append(func_node)
                edges.append(ContainsEdge.create(
                    self.organization_id, parent_urn, func_urn,
                ))

            elif kind in config.transparent_kinds:
                body_field = config.transparent_kinds[kind]
                body = actual.field(body_field)
                if body:
                    sub_nodes, sub_edges = self._extract_from_scope(
                        body, config, parent_urn, root_name, rel_path,
                        name_chain, language,
                    )
                    nodes.extend(sub_nodes)
                    edges.extend(sub_edges)

        return nodes, edges

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _unwrap_node(node, config: LanguageConfig):
        """Unwrap decorator/annotation wrappers to get the inner definition."""
        kind = node.kind()
        if kind in config.wrapper_kinds:
            inner_field = config.wrapper_kinds[kind]
            inner = node.field(inner_field)
            if inner:
                return inner
        return node

    @staticmethod
    def _disambiguate_name(name: str, name_counts: dict[str, int]) -> str:
        """Append ordinal suffix for duplicate names (e.g. overloaded methods)."""
        if name in name_counts:
            name_counts[name] += 1
            return f"{name}__{name_counts[name]}"
        name_counts[name] = 1
        return name

    @staticmethod
    def _extract_base_classes(class_node) -> list[str]:
        """Extract base class names from a class definition node."""
        bases = []
        # Python: argument_list contains base classes
        arg_list = class_node.field("superclasses")
        if arg_list:
            for child in arg_list.children():
                if child.is_named():
                    bases.append(child.text())
            return bases
        # JS/TS/Java: class_heritage or superclass field
        heritage = class_node.field("superclass")
        if heritage:
            bases.append(heritage.text())
        return bases
