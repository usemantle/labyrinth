"""
Plugin interface for enriching codebase nodes during scanning.

Plugins are passed to CodebaseLoader via the ``plugins`` constructor
parameter.  All enrichment happens in the ``post_process`` hook, which
runs after all files have been scanned and after language analyzers
have completed.  This gives plugins access to the full symbol table
and file sources, enabling import-gated enrichment that avoids
cross-pollination between unrelated frameworks.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

from labyrinth.graph.graph_models import Edge, Node, NodeMetadataKey

if TYPE_CHECKING:
    from labyrinth.graph.loaders.codebase.codebase_loader import PostProcessContext


class CodebasePlugin:
    """Base class for codebase loader plugins.

    All domain-specific enrichment should be done in ``post_process``.
    Use the ``_file_imports_library`` helper to gate enrichment by
    whether a file actually imports the relevant framework.
    """

    _DEP_FILES: tuple[str, ...] = (
        "pyproject.toml",
        "requirements.txt",
        "requirements.in",
        "setup.py",
        "setup.cfg",
    )

    @staticmethod
    def _dependency_mentions(root_path: Path, package_name: str) -> bool:
        """Check whether *package_name* appears in any dependency file."""
        for name in CodebasePlugin._DEP_FILES:
            dep_file = root_path / name
            if dep_file.is_file():
                try:
                    content = dep_file.read_text(encoding="utf-8", errors="replace")
                except OSError:
                    continue
                if package_name in content:
                    return True
        return False

    @staticmethod
    def _file_imports_library(source: str, library: str) -> bool:
        """Check whether a source file imports from *library*.

        Matches both ``import library`` and ``from library... import ...``
        patterns.  This is a fast regex check suitable for gating plugin
        enrichment to files that actually use the framework.
        """
        pattern = r"^(?:from\s+|import\s+)" + re.escape(library) + r"\b"
        return bool(re.search(pattern, source, re.MULTILINE))

    @staticmethod
    def _get_node_source(
        node: Node,
        context: PostProcessContext,
    ) -> str | None:
        """Extract a node's source text from file sources using line numbers.

        Works for both function and class nodes.  Returns None if the
        source cannot be determined.
        """
        rel_path = node.metadata.get(NodeMetadataKey.FILE_PATH)
        start = node.metadata.get(NodeMetadataKey.START_LINE)
        end = node.metadata.get(NodeMetadataKey.END_LINE)
        if not rel_path or start is None or end is None:
            return None
        source = context.file_sources.get(rel_path)
        if not source:
            return None
        lines = source.splitlines()
        return "\n".join(lines[start:end + 1])

    @classmethod
    def auto_detect(cls, root_path: Path) -> bool:
        """Return True if this plugin is relevant to the project at *root_path*.

        Default implementation returns False.  Subclasses should override.
        """
        return False

    def supported_languages(self) -> set[str]:
        """Return the set of languages this plugin supports.

        Returns None if the plugin handles all languages (the plugin
        is responsible for filtering in its hooks). Returns a set of
        language names (e.g. ``{"python"}``) to restrict when the
        loader invokes this plugin.
        """
        return set()

    def post_process(
        self,
        nodes: list[Node],
        edges: list[Edge],
        context: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        """Called after all files are scanned and language analyzers have run.

        Receives the full graph and post-processing context. Can add,
        modify, or remove nodes and edges for domain-specific enrichment.

        Args:
            nodes: All discovered nodes.
            edges: All discovered edges.
            context: Post-processing context with file sources, URN builder, etc.

        Returns:
            The (possibly modified) (nodes, edges) tuple.
        """
        return nodes, edges
