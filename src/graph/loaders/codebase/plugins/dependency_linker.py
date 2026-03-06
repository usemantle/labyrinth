"""ABC for dependency linker plugins.

Dependency linkers connect code files to dependency nodes via DEPENDS_ON
edges. The base class handles the shared logic of matching imports to
dependency nodes; subclasses provide language-specific import extraction
and package-to-import-name resolution.
"""

from __future__ import annotations

import abc
import logging
from typing import TYPE_CHECKING

from src.graph.graph_models import (
    URN,
    Edge,
    EdgeMetadata,
    EdgeMetadataKey,
    Node,
    NodeMetadataKey,
    RelationType,
)
from src.graph.loaders._helpers import make_edge
from src.graph.loaders.codebase.plugins._base import CodebasePlugin

if TYPE_CHECKING:
    from src.graph.loaders.codebase.codebase_loader import PostProcessContext

logger = logging.getLogger(__name__)

NK = NodeMetadataKey
EK = EdgeMetadataKey


class DependencyLinkerPlugin(CodebasePlugin, abc.ABC):
    """Base class for language-specific dependency linkers.

    Subclasses must implement:
    - ``language``: the language identifier (e.g. ``"python"``)
    - ``extract_imports``: parse import names from source code
    - ``resolve_import_names``: map a package name to its import name(s)
    """

    @abc.abstractmethod
    def language(self) -> str:
        """Return the language this linker supports (e.g. 'python')."""

    @abc.abstractmethod
    def extract_imports(self, source: str) -> set[str]:
        """Extract top-level import names from a source file.

        Returns:
            Set of lowercased import module names.
        """

    @abc.abstractmethod
    def resolve_import_names(
        self,
        package_name: str,
        context: PostProcessContext,
    ) -> set[str]:
        """Map a package name to the import name(s) it provides.

        Args:
            package_name: The distribution/package name (e.g. 'Pillow').
            context: Post-processing context with root_path, etc.

        Returns:
            Set of lowercased import names (e.g. {'pil'} for Pillow).
        """

    def post_process(
        self,
        nodes: list[Node],
        edges: list[Edge],
        context: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        # Step 1: Build import_name → dep_node mapping
        dep_map: dict[str, Node] = {}
        for node in nodes:
            pkg_name = node.metadata.get(NK.PACKAGE_NAME)
            if pkg_name:
                for import_name in self.resolve_import_names(pkg_name, context):
                    dep_map[import_name.lower()] = node

        if not dep_map:
            return nodes, edges

        # Step 2: Build file URN lookup
        file_urn_map: dict[str, str] = {}
        for node in nodes:
            fp = node.metadata.get(NK.FILE_PATH)
            if fp and NK.FUNCTION_NAME not in node.metadata and NK.CLASS_NAME not in node.metadata:
                file_urn_map[fp] = str(node.urn)

        # Step 3: Scan files for imports and create DEPENDS_ON edges
        new_edges: list[Edge] = []
        linked = 0
        lang = self.language()

        for rel_path, source in context.file_sources.items():
            if context.file_languages.get(rel_path) != lang:
                continue

            file_urn_str = file_urn_map.get(rel_path)
            if not file_urn_str:
                continue

            import_names = self.extract_imports(source)

            for import_name in import_names:
                dep_node = dep_map.get(import_name)
                if dep_node:
                    edge = make_edge(
                        context.organization_id,
                        URN(file_urn_str),
                        dep_node.urn,
                        RelationType.DEPENDS_ON,
                        metadata=EdgeMetadata({
                            EK.IMPORT_NAME: import_name,
                        }),
                    )
                    new_edges.append(edge)
                    linked += 1

        logger.info("Dependency linker [%s]: created %d DEPENDS_ON edges", lang, linked)
        return nodes, edges + new_edges
