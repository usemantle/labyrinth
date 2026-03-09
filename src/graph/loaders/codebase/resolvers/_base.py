"""Abstract base for language-specific cross-file analysis.

Language analyzers are core loader infrastructure — every language has
imports and function calls. They run automatically after structural
extraction, before plugin post_process hooks.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.graph.graph_models import Edge, Node

if TYPE_CHECKING:
    from src.graph.loaders.codebase.codebase_loader import PostProcessContext


@dataclass
class ResolvedImport:
    """A symbol imported into a file, resolved to its source location."""

    source_file: str
    """Relative path of the file where the symbol is defined."""
    source_name: str
    """Name of the symbol in the source file."""
    module_path: str
    """Original dotted module path from the import statement."""
    is_external: bool
    """True if the symbol comes from outside the scanned codebase."""


@dataclass
class CallSite:
    """A function or class call found inside a function body."""

    callee_name: str
    """Name as it appears in source (e.g. 'get_user_by_id')."""
    call_type: str
    """'function_call' or 'class_instantiation'."""
    line: int
    """Line number of the call."""


class LanguageAnalyzer(abc.ABC):
    """Cross-file analysis for a specific programming language.

    Responsible for:
    1. Building an import map (symbol → source file/name)
    2. Extracting call sites from function bodies
    3. Creating CODE_TO_CODE edges by matching calls to resolved symbols
    """

    @abc.abstractmethod
    def analyze(
        self,
        nodes: list[Node],
        edges: list[Edge],
        file_sources: dict[str, str],
        context: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        """Run cross-file analysis and return updated nodes and edges.

        Args:
            nodes: All nodes discovered so far.
            edges: All edges discovered so far.
            file_sources: Mapping of rel_path → source text for files
                of this language.
            context: Full post-processing context.

        Returns:
            Updated (nodes, edges) with CODE_TO_CODE edges added.
        """

    @abc.abstractmethod
    def build_import_map(
        self,
        file_sources: dict[str, str],
        package_name: str,
    ) -> dict[str, dict[str, ResolvedImport]]:
        """Build the import symbol table for all files.

        Args:
            file_sources: Mapping of rel_path → source text.
            package_name: Root package name (e.g. 'api_server').

        Returns:
            {file_rel_path: {local_name: ResolvedImport}}
        """

    @abc.abstractmethod
    def extract_calls(self, function_source: str) -> list[CallSite]:
        """Extract function/class calls from a function's source text.

        Args:
            function_source: Source text of the function (including decorators).

        Returns:
            List of call sites found in the function body.
        """

    def link_dependencies(
        self,
        nodes: list[Node],
        edges: list[Edge],
        file_sources: dict[str, str],
        context: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        """Link source files to dependency nodes via import analysis.

        Runs after plugin post_process hooks, so dependency nodes
        (from any package manager plugin) are available. Override in
        subclasses that support dependency linking.
        """
        return nodes, edges
