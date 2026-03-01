"""
FastAPI plugin for detecting API entrypoints and resolving route paths.

Phase 1 (on_function_node): Detects @router.get/post/etc decorators and
adds http_method, route_path, api_framework, router_variable metadata.

Phase 2 (post_process): Resolves full route paths by combining:
- include_router prefix (from main.py)
- APIRouter prefix (from router declaration)
- decorator path (from @router.get("/path"))
"""

from __future__ import annotations

import re
import logging
from typing import TYPE_CHECKING

from ast_grep_py import SgRoot

from src.graph.graph_models import Node, NodeMetadataKey
from src.graph.loaders.codebase.plugins._base import CodebasePlugin

if TYPE_CHECKING:
    from src.graph.graph_models import Edge
    from src.graph.loaders.codebase.codebase_loader import PostProcessContext

logger = logging.getLogger(__name__)

# Matches @router.get("/{user_id}"), @app.post("/users"), etc.
_ROUTE_RE = re.compile(
    r"@(\w+)\.(get|post|put|patch|delete|head|options)\("
    r'\s*["\']([^"\']*)["\']',
)

# Matches router = APIRouter(prefix="/users") or APIRouter()
_APIROUTER_RE = re.compile(
    r"(\w+)\s*=\s*APIRouter\("
    r'(?:[^)]*prefix\s*=\s*["\']([^"\']*)["\'])?',
)

# Matches app.include_router(alias, prefix="/v1")
_INCLUDE_ROUTER_RE = re.compile(
    r"\.include_router\(\s*(\w+)"
    r'(?:[^)]*prefix\s*=\s*["\']([^"\']*)["\'])?',
)


class FastAPIPlugin(CodebasePlugin):
    """Detects FastAPI route decorators and resolves full route paths."""

    def on_function_node(
        self,
        node: Node,
        function_source: str,
        language: str,
    ) -> Node:
        if language != "python":
            return node

        match = _ROUTE_RE.search(function_source)
        if match:
            router_var, http_method, route_path = match.groups()
            node.metadata[NodeMetadataKey.HTTP_METHOD] = http_method.upper()
            node.metadata[NodeMetadataKey.ROUTE_PATH] = route_path
            node.metadata[NodeMetadataKey.API_FRAMEWORK] = "fastapi"
            node.metadata[NodeMetadataKey.ROUTER_VARIABLE] = router_var

        return node

    def post_process(
        self,
        nodes: list[Node],
        edges: list[Edge],
        context: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        """Resolve full route paths by combining router and include_router prefixes."""
        # 1. Find APIRouter declarations: {rel_path: {var_name: prefix}}
        router_prefixes = _find_router_prefixes(context.file_sources)

        # 2. Find include_router calls and resolve aliases via import map
        include_prefixes = _find_include_router_prefixes(
            context.file_sources, context.root_name,
        )

        # 3. Resolve full_route_path for each endpoint
        resolved = 0
        for node in nodes:
            if NodeMetadataKey.ROUTE_PATH not in node.metadata:
                continue

            rel_path = node.metadata.get(NodeMetadataKey.FILE_PATH)
            router_var = node.metadata.get(NodeMetadataKey.ROUTER_VARIABLE)
            route_path = node.metadata[NodeMetadataKey.ROUTE_PATH]

            if not rel_path or not router_var:
                continue

            # Get the APIRouter prefix for this variable
            file_routers = router_prefixes.get(rel_path, {})
            router_prefix = file_routers.get(router_var, "")

            # Get include_router prefix for this file's router
            include_prefix = include_prefixes.get(rel_path, "")

            full_path = include_prefix + router_prefix + route_path
            node.metadata[NodeMetadataKey.FULL_ROUTE_PATH] = full_path
            resolved += 1

        logger.info("FastAPI plugin: resolved %d full route paths", resolved)
        return nodes, edges


def _find_router_prefixes(
    file_sources: dict[str, str],
) -> dict[str, dict[str, str]]:
    """Find APIRouter(prefix=...) declarations in all files.

    Returns:
        {rel_path: {variable_name: prefix_string}}
    """
    result: dict[str, dict[str, str]] = {}
    for rel_path, source in file_sources.items():
        for match in _APIROUTER_RE.finditer(source):
            var_name = match.group(1)
            prefix = match.group(2) or ""
            result.setdefault(rel_path, {})[var_name] = prefix
    return result


def _find_include_router_prefixes(
    file_sources: dict[str, str],
    package_name: str,
) -> dict[str, str]:
    """Find include_router() calls and map source files to their prefixes.

    Returns:
        {source_rel_path: prefix} — the prefix that include_router applies
        to the router imported from source_rel_path.
    """
    result: dict[str, str] = {}

    for rel_path, source in file_sources.items():
        # First build import map for this file to resolve aliases
        imports = _parse_simple_imports(source, package_name, file_sources)

        for match in _INCLUDE_ROUTER_RE.finditer(source):
            alias = match.group(1)
            prefix = match.group(2) or ""

            # Resolve the alias to the source file
            if alias in imports:
                source_file = imports[alias]
                if source_file:
                    result[source_file] = prefix

    return result


def _parse_simple_imports(
    source: str,
    package_name: str,
    all_files: dict[str, str],
) -> dict[str, str | None]:
    """Quick import parsing to resolve aliases for include_router.

    Returns:
        {local_name: source_rel_path_or_None}
    """
    imports: dict[str, str | None] = {}
    try:
        root = SgRoot(source, "python")
        ast_root = root.root()
    except Exception:
        return imports

    for child in ast_root.children():
        if child.kind() != "import_from_statement":
            continue
        module_node = child.field("module_name")
        if not module_node:
            continue
        module_path = module_node.text()

        # Resolve to rel_path
        source_file = _module_to_path(module_path, package_name, all_files)

        for name_child in child.children():
            if name_child.kind() == "aliased_import":
                alias_node = name_child.field("alias")
                local = alias_node.text() if alias_node else name_child.field("name").text()
                imports[local] = source_file
            elif name_child.kind() == "dotted_name" and name_child.text() != module_path:
                imports[name_child.text()] = source_file

    return imports


def _module_to_path(
    module_path: str,
    package_name: str,
    all_files: dict[str, str],
) -> str | None:
    """Convert dotted module path to rel_path (same logic as python resolver)."""
    if not module_path.startswith(package_name):
        return None
    suffix = module_path[len(package_name):]
    if suffix.startswith("."):
        suffix = suffix[1:]
    if not suffix:
        return "__init__.py" if "__init__.py" in all_files else None
    path_str = suffix.replace(".", "/")
    candidate = f"{path_str}.py"
    if candidate in all_files:
        return candidate
    candidate = f"{path_str}/__init__.py"
    if candidate in all_files:
        return candidate
    return None
