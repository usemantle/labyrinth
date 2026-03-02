"""Rust-specific import resolution and call graph extraction.

Handles:
- Crate-absolute imports (``use crate::models::user::User``)
- Relative imports (``use super::helper``, ``use self::submod::Item``)
- Grouped imports (``use crate::models::{User, Order}``)
- Aliases (``use crate::models::User as U``)
- Simple function calls (``foo()``) and path calls (``Foo::new()``)
"""

from __future__ import annotations

import logging
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

from ast_grep_py import SgRoot

from src.graph.graph_models import (
    Edge,
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


class RustAnalyzer(LanguageAnalyzer):
    """Cross-file analysis for Rust codebases."""

    def analyze(
        self,
        nodes: list[Node],
        edges: list[Edge],
        file_sources: dict[str, str],
        context: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        """Build import map, extract calls, create CODE_TO_CODE edges."""
        import_map = self.build_import_map(file_sources, context.root_name)

        func_index = _build_function_index(nodes)
        funcs_by_file = _build_funcs_by_file(nodes)

        new_edges: list[Edge] = []
        for node in nodes:
            if NodeMetadataKey.FUNCTION_NAME not in node.metadata:
                continue
            rel_path = node.metadata.get(NodeMetadataKey.FILE_PATH)
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
                    func_index, rel_path, file_sources,
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
        logger.info("Rust analyzer: added %d CODE_TO_CODE edges", len(new_edges))
        return nodes, edges

    def build_import_map(
        self,
        file_sources: dict[str, str],
        package_name: str,
    ) -> dict[str, dict[str, ResolvedImport]]:
        """Parse ``use`` declarations from all Rust files."""
        result: dict[str, dict[str, ResolvedImport]] = {}
        for rel_path, source in file_sources.items():
            imports = self._parse_use_declarations(
                rel_path, source, package_name, file_sources,
            )
            if imports:
                result[rel_path] = imports
        return result

    def extract_calls(self, function_source: str) -> list[CallSite]:
        """Extract function/class calls from Rust function source."""
        try:
            root = SgRoot(function_source, "rust")
            ast_root = root.root()
        except Exception:
            return []

        calls: list[CallSite] = []
        _walk_for_calls(ast_root, calls)
        return calls

    # ------------------------------------------------------------------
    # Use-statement parsing
    # ------------------------------------------------------------------

    def _parse_use_declarations(
        self,
        rel_path: str,
        source: str,
        package_name: str,
        all_files: dict[str, str],
    ) -> dict[str, ResolvedImport]:
        """Parse all ``use`` declarations in a single file."""
        try:
            root = SgRoot(source, "rust")
            ast_root = root.root()
        except Exception:
            return {}

        imports: dict[str, ResolvedImport] = {}
        for child in ast_root.children():
            if child.kind() == "use_declaration":
                self._handle_use_decl(
                    child, rel_path, package_name, all_files, imports,
                )
        return imports

    def _handle_use_decl(
        self,
        node,
        rel_path: str,
        package_name: str,
        all_files: dict[str, str],
        imports: dict[str, ResolvedImport],
    ) -> None:
        """Handle a single ``use`` declaration."""
        # Get the use-statement content (everything between ``use`` and ``;``).
        text = node.text().strip()
        if text.startswith("use "):
            text = text[4:]
        if text.endswith(";"):
            text = text[:-1]
        text = text.strip()

        if not text:
            return

        for module_path, local_name, original_name in _extract_use_items(text):
            source_file = _resolve_rust_module(
                module_path, rel_path, all_files,
            )
            is_external = source_file is None
            imports[local_name] = ResolvedImport(
                source_file=source_file or "",
                source_name=original_name,
                module_path=module_path,
                is_external=is_external,
            )


# ── Use-statement text parsing ───────────────────────────────────────


def _extract_use_items(text: str) -> list[tuple[str, str, str]]:
    """Parse a use-statement body into (module_path, local_name, original_name) tuples.

    Handles:
    - ``crate::foo::Bar``
    - ``crate::foo::Bar as B``
    - ``crate::foo::{Bar, Baz}``
    - ``crate::foo::{Bar as B, Baz}``
    """
    # Handle grouped imports: prefix::{A, B as C}
    brace_start = text.find("::{")
    if brace_start != -1 and text.endswith("}"):
        prefix = text[:brace_start]
        group_text = text[brace_start + 3:-1]  # strip ::{ and }
        items: list[tuple[str, str, str]] = []
        for item in group_text.split(","):
            item = item.strip()
            if not item:
                continue
            if " as " in item:
                original, _, alias = item.partition(" as ")
                items.append((prefix, alias.strip(), original.strip()))
            else:
                items.append((prefix, item, item))
        return items

    # Handle alias: foo::bar::Baz as B
    if " as " in text:
        path_part, _, alias = text.partition(" as ")
        parts = path_part.rsplit("::", 1)
        if len(parts) == 2:
            module_path, original = parts
        else:
            module_path, original = "", parts[0]
        return [(module_path, alias.strip(), original.strip())]

    # Simple import: foo::bar::Baz
    parts = text.rsplit("::", 1)
    if len(parts) == 2:
        module_path, name = parts
        return [(module_path, name, name)]

    # Bare identifier
    return [("", text, text)]


# ── Module path resolution ───────────────────────────────────────────


def _resolve_rust_module(
    module_path: str,
    rel_path: str,
    all_files: dict[str, str],
) -> str | None:
    """Resolve a Rust module path to a rel_path in the scanned codebase.

    Handles ``crate::``, ``super::``, and ``self::`` prefixes.
    External crate imports return None.
    """
    if module_path.startswith("crate"):
        suffix = module_path[len("crate"):]
        if suffix.startswith("::"):
            suffix = suffix[2:]
        path_str = suffix.replace("::", "/") if suffix else ""
    elif module_path.startswith("super"):
        current_dir = PurePosixPath(rel_path).parent
        suffix = module_path[len("super"):]
        if suffix.startswith("::"):
            suffix = suffix[2:]
        target_dir = current_dir.parent
        path_str = str(target_dir / suffix.replace("::", "/")) if suffix else str(target_dir)
    elif module_path.startswith("self"):
        current_dir = PurePosixPath(rel_path).parent
        suffix = module_path[len("self"):]
        if suffix.startswith("::"):
            suffix = suffix[2:]
        path_str = str(current_dir / suffix.replace("::", "/")) if suffix else str(current_dir)
    else:
        # Bare path — try as crate-relative before giving up.
        path_str = module_path.replace("::", "/")
        for candidate in _rust_module_candidates(path_str):
            if candidate in all_files:
                return candidate
        return None

    if not path_str or path_str == ".":
        return None

    # Try common Rust file candidates.
    for candidate in _rust_module_candidates(path_str):
        if candidate in all_files:
            return candidate

    return None


def _rust_module_candidates(path_str: str) -> list[str]:
    """Generate candidate file paths for a Rust module path segment."""
    candidates = [
        f"{path_str}.rs",
        f"{path_str}/mod.rs",
        f"src/{path_str}.rs",
        f"src/{path_str}/mod.rs",
    ]
    return candidates


# ── Helper functions ─────────────────────────────────────────────────


def _build_function_index(nodes: list[Node]) -> dict[tuple[str, str], Node]:
    """Build (rel_path, function_name) -> Node lookup."""
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
    """Build rel_path -> [function nodes] lookup."""
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
    return "\n".join(lines[start:end + 1])


def _resolve_call_target(
    call: CallSite,
    file_imports: dict[str, ResolvedImport],
    same_file_funcs: set[str],
    func_index: dict[tuple[str, str], Node],
    current_file: str,
    all_files: dict[str, str],
) -> Node | None:
    """Resolve a call site to a target function Node."""
    name = call.callee_name

    # 1. Qualified path calls (e.g. crate::foo::bar::func_name)
    if "::" in name:
        target = _resolve_qualified_call(name, func_index, all_files, current_file)
        if target:
            return target
        # Fall back to the last segment for import / same-file lookup.
        name = name.rsplit("::", 1)[-1]

    # 2. Check imports (cross-file)
    if name in file_imports:
        imp = file_imports[name]
        if imp.is_external:
            return None
        target = func_index.get((imp.source_file, imp.source_name))
        if target:
            return target

    # 3. Check same-file definitions
    if name in same_file_funcs:
        target = func_index.get((current_file, name))
        if target:
            return target

    return None


def _resolve_qualified_call(
    qualified_name: str,
    func_index: dict[tuple[str, str], Node],
    all_files: dict[str, str],
    current_file: str,
) -> Node | None:
    """Resolve a fully-qualified call like ``crate::cli::resolve::resolve_vault()``.

    Tries progressively shorter prefixes as the module path while always
    using the *last* segment of the original path as the function name.
    This handles both ``crate::mod::func()`` and ``crate::mod::Type::method()``.
    """
    parts = qualified_name.split("::")
    func_name = parts[-1]

    for i in range(len(parts) - 1, 0, -1):
        module_path = "::".join(parts[:i])
        source_file = _resolve_rust_module(module_path, current_file, all_files)
        if source_file:
            target = func_index.get((source_file, func_name))
            if target:
                return target

    return None


def _walk_for_calls(node, calls: list[CallSite]) -> None:
    """Recursively walk AST to find function/class calls."""
    if node.kind() == "call_expression":
        func_node = node.field("function")
        if func_node:
            name = func_node.text()
            # Skip method calls (field access like obj.method())
            if "." in name:
                pass
            elif "::" in name:
                # Store full qualified path — resolver handles splitting.
                last = name.rsplit("::", 1)[-1]
                call_type = (
                    "class_instantiation"
                    if last[:1].isupper()
                    else "function_call"
                )
                calls.append(CallSite(
                    callee_name=name,
                    call_type=call_type,
                    line=node.range().start.line,
                ))
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

    # Inside macro token trees, function calls appear as identifier + token_tree
    # rather than call_expression (e.g. assert!(func(x), "msg")).
    if node.kind() == "token_tree":
        _extract_calls_from_token_tree(node, calls)

    for child in node.children():
        _walk_for_calls(child, calls)


def _extract_calls_from_token_tree(node, calls: list[CallSite]) -> None:
    """Detect function calls inside macro token trees.

    In Rust's tree-sitter, ``assert!(my_func(x), "msg")`` is parsed as a
    ``macro_invocation`` whose ``token_tree`` contains ``identifier``
    (``my_func``) followed by ``token_tree`` (``(x)``), NOT as a
    ``call_expression``.  This function walks the token tree's children
    looking for that pattern.
    """
    children = node.children()
    i = 0
    while i < len(children):
        child = children[i]
        if child.kind() == "identifier" and i + 1 < len(children):
            next_child = children[i + 1]
            # Skip nested macros: identifier followed by "!" (e.g. format!(...))
            if next_child.kind() == "!":
                i += 1
            elif next_child.kind() == "token_tree" and next_child.text().startswith("("):
                name = child.text()
                if "." not in name:
                    call_type = (
                        "class_instantiation"
                        if name[:1].isupper()
                        else "function_call"
                    )
                    calls.append(CallSite(
                        callee_name=name,
                        call_type=call_type,
                        line=child.range().start.line,
                    ))
        # Recurse into nested token trees for deeply nested calls.
        if child.kind() == "token_tree":
            _extract_calls_from_token_tree(child, calls)
        i += 1
