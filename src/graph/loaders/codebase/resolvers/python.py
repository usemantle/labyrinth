"""Python-specific import resolution and call graph extraction.

Handles:
- Absolute imports (``from api_server.models.X import Y``)
- Relative imports (``from .dependencies import Z``)
- Aliases (``import X as Y``)
- Simple function calls (``foo()``) and class instantiation (``Foo()``)
"""

from __future__ import annotations

import logging
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

from ast_grep_py import SgRoot

from src.graph.graph_models import (
    Edge,
    EdgeMetadata,
    EdgeMetadataKey,
    Node,
    NodeMetadataKey,
    RelationType,
)
from src.graph.loaders._helpers import make_edge
from src.graph.loaders.codebase.resolvers._base import (
    CallSite,
    LanguageAnalyzer,
    ResolvedImport,
)

if TYPE_CHECKING:
    from src.graph.loaders.codebase.codebase_loader import PostProcessContext

logger = logging.getLogger(__name__)


class PythonAnalyzer(LanguageAnalyzer):
    """Cross-file analysis for Python codebases."""

    def analyze(
        self,
        nodes: list[Node],
        edges: list[Edge],
        file_sources: dict[str, str],
        context: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        """Build import map, extract calls, create CODE_TO_CODE edges."""
        import_map = self.build_import_map(file_sources, context.root_name)

        # Build lookup: (rel_path, function_name) → Node
        func_index = _build_function_index(nodes)

        # Build lookup: rel_path → [function nodes in that file]
        funcs_by_file = _build_funcs_by_file(nodes)

        new_edges: list[Edge] = []
        for node in nodes:
            if NodeMetadataKey.FUNCTION_NAME not in node.metadata:
                continue
            rel_path = node.metadata.get(NodeMetadataKey.FILE_PATH)
            func_name = node.metadata[NodeMetadataKey.FUNCTION_NAME]
            if not rel_path or rel_path not in file_sources:
                continue

            func_source = _get_function_source(node, file_sources[rel_path])
            if not func_source:
                continue

            calls = self.extract_calls(func_source)
            file_imports = import_map.get(rel_path, {})
            same_file_funcs = {
                n.metadata[NodeMetadataKey.FUNCTION_NAME]
                for n in funcs_by_file.get(rel_path, [])
                if n is not node
            }

            for call in calls:
                target = _resolve_call_target(
                    call, file_imports, same_file_funcs,
                    func_index, rel_path,
                )
                if target is None:
                    continue
                edge = make_edge(
                    context.organization_id,
                    node.urn, target.urn,
                    RelationType.CODE_TO_CODE,
                )
                edge.metadata[EdgeMetadataKey.CALL_TYPE] = call.call_type
                new_edges.append(edge)

        edges = edges + new_edges
        logger.info("Python analyzer: added %d CODE_TO_CODE edges", len(new_edges))

        # Stdlib IO detection
        from src.graph.loaders.codebase.resolvers.python_stdlib import enrich_stdlib_io
        nodes = enrich_stdlib_io(nodes, file_sources)

        return nodes, edges

    def build_import_map(
        self,
        file_sources: dict[str, str],
        package_name: str,
    ) -> dict[str, dict[str, ResolvedImport]]:
        """Parse import statements from all Python files."""
        result: dict[str, dict[str, ResolvedImport]] = {}
        for rel_path, source in file_sources.items():
            imports = self._parse_imports(rel_path, source, package_name, file_sources)
            if imports:
                result[rel_path] = imports
        return result

    def extract_calls(self, function_source: str) -> list[CallSite]:
        """Extract simple function/class calls from function source."""
        try:
            root = SgRoot(function_source, "python")
            ast_root = root.root()
        except Exception:
            return []

        calls: list[CallSite] = []
        _walk_for_calls(ast_root, calls)
        return calls

    # ------------------------------------------------------------------
    # Import parsing
    # ------------------------------------------------------------------

    def _parse_imports(
        self,
        rel_path: str,
        source: str,
        package_name: str,
        all_files: dict[str, str],
    ) -> dict[str, ResolvedImport]:
        """Parse all import statements in a single file."""
        try:
            root = SgRoot(source, "python")
            ast_root = root.root()
        except Exception:
            return {}

        imports: dict[str, ResolvedImport] = {}
        for child in ast_root.children():
            kind = child.kind()
            if kind == "import_from_statement":
                self._handle_from_import(
                    child, rel_path, package_name, all_files, imports,
                )
        return imports

    def _handle_from_import(
        self,
        node,
        rel_path: str,
        package_name: str,
        all_files: dict[str, str],
        imports: dict[str, ResolvedImport],
    ) -> None:
        """Handle ``from X import Y [as Z]`` statements."""
        module_node = node.field("module_name")
        if not module_node:
            return

        module_path = module_node.text()

        # Check for relative imports (leading dots)
        is_relative = module_path.startswith(".")
        if is_relative:
            resolved_module = _resolve_relative_import(
                module_path, rel_path,
            )
        else:
            resolved_module = module_path

        # Try to resolve to a file in the codebase
        source_file = _module_to_rel_path(
            resolved_module, package_name, all_files,
        )
        is_external = source_file is None

        # Extract imported names
        for child in node.children():
            if child.kind() in ("dotted_name", "aliased_import"):
                local_name, original_name = _extract_import_name(child)
                imports[local_name] = ResolvedImport(
                    source_file=source_file or "",
                    source_name=original_name,
                    module_path=module_path,
                    is_external=is_external,
                )


# ── Helper functions ─────────────────────────────────────────────────


def _build_function_index(nodes: list[Node]) -> dict[tuple[str, str], Node]:
    """Build (rel_path, function_name) → Node lookup."""
    index: dict[tuple[str, str], Node] = {}
    for node in nodes:
        if NodeMetadataKey.FUNCTION_NAME not in node.metadata:
            continue
        rel_path = node.metadata.get(NodeMetadataKey.FILE_PATH)
        func_name = node.metadata[NodeMetadataKey.FUNCTION_NAME]
        if rel_path:
            index[(rel_path, func_name)] = node
    return index


def _build_funcs_by_file(nodes: list[Node]) -> dict[str, list[Node]]:
    """Build rel_path → [function nodes] lookup."""
    by_file: dict[str, list[Node]] = {}
    for node in nodes:
        if NodeMetadataKey.FUNCTION_NAME not in node.metadata:
            continue
        rel_path = node.metadata.get(NodeMetadataKey.FILE_PATH)
        if rel_path:
            by_file.setdefault(rel_path, []).append(node)
    return by_file


def _get_function_source(node: Node, file_source: str) -> str | None:
    """Extract a function's source text from its file using line numbers."""
    start = node.metadata.get(NodeMetadataKey.START_LINE)
    end = node.metadata.get(NodeMetadataKey.END_LINE)
    if start is None or end is None:
        return None
    lines = file_source.splitlines()
    # START_LINE and END_LINE are 0-indexed from ast-grep
    return "\n".join(lines[start:end + 1])


def _resolve_call_target(
    call: CallSite,
    file_imports: dict[str, ResolvedImport],
    same_file_funcs: set[str],
    func_index: dict[tuple[str, str], Node],
    current_file: str,
) -> Node | None:
    """Resolve a call site to a target function Node."""
    name = call.callee_name

    # 1. Check imports (cross-file)
    if name in file_imports:
        imp = file_imports[name]
        if imp.is_external:
            return None
        target = func_index.get((imp.source_file, imp.source_name))
        if target:
            return target

    # 2. Check same-file definitions
    if name in same_file_funcs:
        target = func_index.get((current_file, name))
        if target:
            return target

    return None


def _resolve_relative_import(module_path: str, rel_path: str) -> str:
    """Resolve a relative import like '.foo' or '..bar' to an absolute module."""
    # Count leading dots
    dots = 0
    for ch in module_path:
        if ch == ".":
            dots += 1
        else:
            break

    remainder = module_path[dots:]
    current_dir = PurePosixPath(rel_path).parent

    # Go up (dots - 1) levels from current directory
    target_dir = current_dir
    for _ in range(dots - 1):
        target_dir = target_dir.parent

    if remainder:
        return str(target_dir / remainder).replace("/", ".")
    return str(target_dir).replace("/", ".")


def _module_to_rel_path(
    module_path: str,
    package_name: str,
    all_files: dict[str, str],
) -> str | None:
    """Convert a dotted module path to a rel_path in the scanned codebase.

    For ``from api_server.models.user import User`` with
    package_name='api_server', tries:
    1. models/user.py
    2. models/user/__init__.py
    """
    if not module_path.startswith(package_name):
        return None

    # Strip package prefix
    suffix = module_path[len(package_name):]
    if suffix.startswith("."):
        suffix = suffix[1:]

    if not suffix:
        # Importing from the package root itself
        if "__init__.py" in all_files:
            return "__init__.py"
        return None

    # Convert dots to path separators
    path_str = suffix.replace(".", "/")

    # Try as a .py file
    candidate = f"{path_str}.py"
    if candidate in all_files:
        return candidate

    # Try as a package __init__.py
    candidate = f"{path_str}/__init__.py"
    if candidate in all_files:
        return candidate

    return None


def _extract_import_name(node) -> tuple[str, str]:
    """Extract (local_name, original_name) from an import name node.

    Handles both plain names and ``X as Y`` aliases.
    """
    if node.kind() == "aliased_import":
        name_node = node.field("name")
        alias_node = node.field("alias")
        original = name_node.text() if name_node else node.text()
        local = alias_node.text() if alias_node else original
        return local, original
    return node.text(), node.text()


def _walk_for_calls(node, calls: list[CallSite]) -> None:
    """Recursively walk AST to find function/class calls."""
    if node.kind() == "call":
        func_node = node.field("function")
        if func_node:
            name = func_node.text()
            # Skip attribute calls (obj.method()) — needs type inference
            if "." in name:
                pass
            else:
                call_type = (
                    "class_instantiation"
                    if name[:1].isupper()
                    else "function_call"
                )
                calls.append(CallSite(
                    callee_name=name,
                    call_type=call_type,
                    line=node.range().start.line,
                ))

    for child in node.children():
        _walk_for_calls(child, calls)
