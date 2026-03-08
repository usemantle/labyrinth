"""
Plugin interface for enriching codebase nodes during scanning.

Plugins are passed to CodebaseLoader via the ``plugins`` constructor
parameter.  The loader calls each plugin's hook methods after extracting
class and function nodes, allowing plugins to add domain-specific
metadata (e.g. ORM table mappings, API route annotations).

Plugins also have a ``post_process`` hook that runs after all files have
been scanned and after language analyzers have completed.  This allows
plugins to perform cross-file analysis using the full symbol table.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.graph.graph_models import Edge, Node

if TYPE_CHECKING:
    from src.graph.loaders.codebase.codebase_loader import PostProcessContext


class CodebasePlugin:
    """Base class for codebase loader plugins.

    Override the hook methods you care about.  Default implementations
    are no-ops that return the node unchanged.
    """

    def supported_languages(self) -> set[str]:
        """Return the set of languages this plugin supports.

        Returns None if the plugin handles all languages (the plugin
        is responsible for filtering in its hooks). Returns a set of
        language names (e.g. ``{"python"}``) to restrict when the
        loader invokes this plugin.
        """
        return set()

    def on_class_node(
        self,
        node: Node,
        class_body_source: str,
        language: str,
    ) -> Node:
        """Called after a class node is extracted.

        Args:
            node: The class node with existing metadata.
            class_body_source: Source text of the class body.
            language: The ast-grep language name (e.g. "python").

        Returns:
            The (possibly enriched) node.
        """
        return node

    def on_function_node(
        self,
        node: Node,
        function_source: str,
    ) -> Node:
        """Called after a function/method node is extracted.

        Args:
            node: The function node with existing metadata.
            function_source: Source text of the entire function
                definition (including decorators).
            language: The ast-grep language name (e.g. "python").

        Returns:
            The (possibly enriched) node.
        """
        return node

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
