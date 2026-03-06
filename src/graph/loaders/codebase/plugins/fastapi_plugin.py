"""
FastAPI plugin for detecting API entrypoints and resolving route paths.

Phase 1 (on_function_node): Detects @router.get/post/etc decorators and
adds http_method, route_path, api_framework, router_variable metadata.
Also detects HTTPAuthorizationCredentials / HTTPBasicCredentials type
annotations in function parameters for auth scheme tagging.

Phase 2 (post_process): Resolves full route paths by combining:
- include_router prefix (from main.py)
- APIRouter prefix (from router declaration)
- decorator path (from @router.get("/path"))

Also detects FastAPI security scheme instantiations and propagates
auth_scheme / auth_scheme_var to endpoint functions via Depends().
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

NK = NodeMetadataKey

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

# Detects HTTPAuthorizationCredentials or HTTPBasicCredentials in type annotations
_AUTH_CREDENTIALS_RE = re.compile(
    r'\b(HTTPAuthorizationCredentials|HTTPBasicCredentials)\b',
)

# FastAPI security scheme instantiation: var_name = SchemeClass(...)
_SECURITY_SCHEMES = {
    "HTTPBearer", "HTTPBasic",
    "OAuth2PasswordBearer", "OAuth2AuthorizationCodeBearer",
    "APIKeyHeader", "APIKeyQuery", "APIKeyCookie",
}

_SECURITY_SCHEME_RE = re.compile(
    r'(\w+)\s*=\s*(' + '|'.join(_SECURITY_SCHEMES) + r')\s*\(',
)

# Detects Depends(var_name) in function source
_DEPENDS_RE = re.compile(r'Depends\s*\(\s*(\w+)')

# Detects APIRouter(dependencies=[Depends(var_name)])
_ROUTER_DEPS_RE = re.compile(
    r'(\w+)\s*=\s*APIRouter\s*\([^)]*dependencies\s*=\s*\[([^\]]*)\]',
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
            node.metadata[NK.HTTP_METHOD] = http_method.upper()
            node.metadata[NK.ROUTE_PATH] = route_path
            node.metadata[NK.API_FRAMEWORK] = "fastapi"
            node.metadata[NK.ROUTER_VARIABLE] = router_var

        # Detect auth credentials type annotations
        cred_match = _AUTH_CREDENTIALS_RE.search(function_source)
        if cred_match:
            cred_type = cred_match.group(1)
            if cred_type == "HTTPAuthorizationCredentials":
                node.metadata[NK.AUTH_SCHEME] = "HTTPBearer"
            elif cred_type == "HTTPBasicCredentials":
                node.metadata[NK.AUTH_SCHEME] = "HTTPBasic"

        return node

    def post_process(
        self,
        nodes: list[Node],
        edges: list[Edge],
        context: PostProcessContext,
    ) -> tuple[list[Node], list[Edge]]:
        """Resolve full route paths and propagate auth scheme metadata."""
        # 1. Find APIRouter declarations: {rel_path: {var_name: prefix}}
        router_prefixes = _find_router_prefixes(context.file_sources)

        # 2. Find include_router calls and resolve aliases via import map
        include_prefixes = _find_include_router_prefixes(
            context.file_sources, context.root_name,
        )

        # 3. Detect security scheme instantiations across all files
        # {var_name: scheme_class_name}
        scheme_vars = _find_security_schemes(context.file_sources)

        # 4. Detect router-level auth dependencies
        # {rel_path: {router_var: scheme_class_name}}
        router_auth = _find_router_auth(context.file_sources, scheme_vars)

        # 5. Resolve full_route_path and auth_scheme for each endpoint
        resolved = 0
        auth_tagged = 0
        for node in nodes:
            if NK.ROUTE_PATH not in node.metadata:
                continue

            rel_path = node.metadata.get(NK.FILE_PATH)
            router_var = node.metadata.get(NK.ROUTER_VARIABLE)
            route_path = node.metadata[NK.ROUTE_PATH]

            if not rel_path or not router_var:
                continue

            # Get the APIRouter prefix for this variable
            file_routers = router_prefixes.get(rel_path, {})
            router_prefix = file_routers.get(router_var, "")

            # Get include_router prefix for this file's router
            include_prefix = include_prefixes.get(rel_path, "")

            full_path = include_prefix + router_prefix + route_path
            node.metadata[NK.FULL_ROUTE_PATH] = full_path
            resolved += 1

            # Auth scheme from Depends(var_name) — more specific than type annotation
            _apply_auth_from_source(node, context, scheme_vars, router_auth)
            if NK.AUTH_SCHEME in node.metadata:
                auth_tagged += 1

        logger.info(
            "FastAPI plugin: resolved %d full route paths, tagged %d auth schemes",
            resolved, auth_tagged,
        )
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


def _find_security_schemes(
    file_sources: dict[str, str],
) -> dict[str, str]:
    """Find security scheme instantiations across all files.

    Returns:
        {variable_name: scheme_class_name}
    """
    result: dict[str, str] = {}
    for source in file_sources.values():
        for match in _SECURITY_SCHEME_RE.finditer(source):
            var_name = match.group(1)
            scheme_class = match.group(2)
            result[var_name] = scheme_class
    return result


def _find_router_auth(
    file_sources: dict[str, str],
    scheme_vars: dict[str, str],
) -> dict[str, dict[str, str]]:
    """Find router-level auth via APIRouter(dependencies=[Depends(var)]).

    Returns:
        {rel_path: {router_var: scheme_class_name}}
    """
    result: dict[str, dict[str, str]] = {}
    for rel_path, source in file_sources.items():
        for match in _ROUTER_DEPS_RE.finditer(source):
            router_var = match.group(1)
            deps_content = match.group(2)
            for dep_match in _DEPENDS_RE.finditer(deps_content):
                dep_var = dep_match.group(1)
                if dep_var in scheme_vars:
                    result.setdefault(rel_path, {})[router_var] = scheme_vars[dep_var]
    return result


def _apply_auth_from_source(
    node: Node,
    context: PostProcessContext,
    scheme_vars: dict[str, str],
    router_auth: dict[str, dict[str, str]],
) -> None:
    """Apply auth_scheme and auth_scheme_var to an endpoint node.

    Checks the function's file source for Depends(scheme_var) references,
    then falls back to router-level auth.
    """
    rel_path = node.metadata.get(NK.FILE_PATH)
    if not rel_path:
        return

    source = context.file_sources.get(rel_path, "")
    func_name = node.metadata.get(NK.FUNCTION_NAME)
    if not func_name:
        return

    # Extract function source from file source using line numbers
    start_line = node.metadata.get(NK.START_LINE)
    end_line = node.metadata.get(NK.END_LINE)
    if start_line is not None and end_line is not None:
        lines = source.splitlines()
        func_source = "\n".join(lines[start_line:end_line + 1])
    else:
        func_source = source

    # Check for Depends(scheme_var) in function source
    for dep_match in _DEPENDS_RE.finditer(func_source):
        dep_var = dep_match.group(1)
        if dep_var in scheme_vars:
            node.metadata[NK.AUTH_SCHEME] = scheme_vars[dep_var]
            node.metadata[NK.AUTH_SCHEME_VAR] = dep_var
            return

    # Fallback: router-level auth
    router_var = node.metadata.get(NK.ROUTER_VARIABLE)
    if router_var and rel_path in router_auth:
        file_router_auth = router_auth[rel_path]
        if router_var in file_router_auth:
            node.metadata[NK.AUTH_SCHEME] = file_router_auth[router_var]


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
