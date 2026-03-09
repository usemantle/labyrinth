"""
End-to-end tests for Rust cross-file import resolution and call graph.

Uses synthetic Rust files:
- lib.rs:              Crate root, re-exports
- handlers.rs:         Handler function that calls service + same-file helper
- services/user_svc.rs: Service function imported by the handler

Asserts:
1. CODE_TO_CODE edge from handle_request -> get_user_by_id (cross-file call)
2. CODE_TO_CODE edge from handle_request -> format_response (same-file call)
3. Struct and enum nodes are discovered
4. Grouped and aliased imports resolve correctly
"""

import uuid
from pathlib import Path

import pytest

from src.graph.graph_models import NodeMetadataKey
from src.graph.loaders.codebase.filesystem_codebase_loader import FileSystemCodebaseLoader

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

NK = NodeMetadataKey

# ── Synthetic codebase ────────────────────────────────────────────────

LIB_RS = """\
pub mod handlers;
pub mod services;
"""

HANDLERS_RS = """\
use crate::services::user_svc::get_user_by_id;

fn format_response(data: &str) -> String {
    format!("ok: {}", data)
}

fn handle_request(user_id: &str) -> String {
    let raw = get_user_by_id(user_id);
    format_response(&raw)
}
"""

SERVICES_MOD_RS = """\
pub mod user_svc;
"""

SERVICES_USER_SVC_RS = """\
pub fn get_user_by_id(user_id: &str) -> String {
    format!("User({})", user_id)
}

pub fn list_users() -> Vec<String> {
    vec![]
}
"""


def _make_rust_crate(tmp_path: Path) -> Path:
    """Create the synthetic Rust codebase on disk."""
    root = tmp_path / "mycrate"
    root.mkdir()

    (root / "lib.rs").write_text(LIB_RS)
    (root / "handlers.rs").write_text(HANDLERS_RS)

    svc_dir = root / "services"
    svc_dir.mkdir()
    (svc_dir / "mod.rs").write_text(SERVICES_MOD_RS)
    (svc_dir / "user_svc.rs").write_text(SERVICES_USER_SVC_RS)

    return root


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture()
def rust_result(tmp_path):
    """Scan the synthetic Rust codebase and return (nodes, edges)."""
    root = _make_rust_crate(tmp_path)
    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    return loader.load(str(root))


# ── Helpers ───────────────────────────────────────────────────────────


def _find_node(nodes, **metadata_match):
    for node in nodes:
        if all(node.metadata.get(k) == v for k, v in metadata_match.items()):
            return node
    keys = ", ".join(f"{k}={v!r}" for k, v in metadata_match.items())
    pytest.fail(f"No node found with metadata: {keys}")


def _find_edges(edges, from_urn=None, to_urn=None, edge_type=None):
    result = []
    for edge in edges:
        if from_urn and str(edge.from_urn) != str(from_urn):
            continue
        if to_urn and str(edge.to_urn) != str(to_urn):
            continue
        if edge_type and edge.edge_type != edge_type:
            continue
        result.append(edge)
    return result


# ── Tests: Structural extraction ──────────────────────────────────────


def test_rust_file_nodes(rust_result):
    """Rust files are discovered as file nodes."""
    nodes, _ = rust_result
    file_paths = {
        n.metadata[NK.FILE_PATH]
        for n in nodes
        if NK.FILE_PATH in n.metadata and NK.CLASS_NAME not in n.metadata
        and NK.FUNCTION_NAME not in n.metadata
    }
    assert "lib.rs" in file_paths
    assert "handlers.rs" in file_paths
    assert "services/user_svc.rs" in file_paths
    assert "services/mod.rs" in file_paths


def test_rust_function_nodes(rust_result):
    """Top-level Rust functions are discovered."""
    nodes, _ = rust_result
    func_names = {
        n.metadata[NK.FUNCTION_NAME]
        for n in nodes
        if NK.FUNCTION_NAME in n.metadata
    }
    assert "handle_request" in func_names
    assert "format_response" in func_names
    assert "get_user_by_id" in func_names
    assert "list_users" in func_names


# ── Tests: Cross-file call graph (CODE_TO_CODE) ──────────────────────


def test_rust_cross_file_call(rust_result):
    """handle_request() calls get_user_by_id() imported from another file."""
    nodes, edges = rust_result

    caller = _find_node(nodes, function_name="handle_request")
    callee = _find_node(nodes, function_name="get_user_by_id")

    code_to_code = _find_edges(
        edges,
        from_urn=caller.urn,
        to_urn=callee.urn,
        edge_type="calls",
    )
    assert len(code_to_code) == 1, (
        f"Expected 1 CODE_TO_CODE edge from handle_request -> get_user_by_id, "
        f"found {len(code_to_code)}"
    )
    assert code_to_code[0].metadata.get("call_type") == "function_call"


def test_rust_same_file_call(rust_result):
    """handle_request() calls format_response() defined in the same file."""
    nodes, edges = rust_result

    caller = _find_node(nodes, function_name="handle_request")
    callee = _find_node(nodes, function_name="format_response")

    code_to_code = _find_edges(
        edges,
        from_urn=caller.urn,
        to_urn=callee.urn,
        edge_type="calls",
    )
    assert len(code_to_code) == 1, (
        f"Expected 1 CODE_TO_CODE edge from handle_request -> format_response, "
        f"found {len(code_to_code)}"
    )


def test_rust_same_file_call_inside_macro(tmp_path):
    """A function calling a same-file helper inside assert!() should produce an edge."""
    root = tmp_path / "repo"
    root.mkdir()
    (root / "vault.rs").write_text(
        "fn is_safe_vault_name(name: &str) -> bool {\n"
        '    !name.is_empty() && name != "." && name != ".." '
        '&& !name.contains(\'/\') && !name.contains(\'\\\\\')\n'
        "}\n"
        "\n"
        "pub fn vault_path(vault: Option<&str>) -> std::path::PathBuf {\n"
        "    match vault {\n"
        '        None | Some("default") => std::path::PathBuf::from("config.toml"),\n'
        "        Some(name) => {\n"
        '            assert!(is_safe_vault_name(name), "unsafe vault name: {}", name);\n'
        '            std::path::PathBuf::from(format!(".vault.{}.toml", name))\n'
        "        }\n"
        "    }\n"
        "}\n"
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, edges = loader.load(str(root))

    caller = _find_node(nodes, function_name="vault_path")
    callee = _find_node(nodes, function_name="is_safe_vault_name")

    code_to_code = _find_edges(
        edges,
        from_urn=caller.urn,
        to_urn=callee.urn,
        edge_type="calls",
    )
    assert len(code_to_code) == 1, (
        f"Expected 1 CODE_TO_CODE edge from vault_path -> is_safe_vault_name, "
        f"found {len(code_to_code)}"
    )


# ── Tests: Struct / enum / trait extraction ───────────────────────────


def test_rust_struct_extraction(tmp_path):
    """Rust structs are discovered as class-like nodes."""
    root = tmp_path / "repo"
    root.mkdir()
    (root / "models.rs").write_text(
        "pub struct User {\n"
        "    pub name: String,\n"
        "    pub email: String,\n"
        "}\n"
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, _ = loader.load(str(root))

    user = _find_node(nodes, class_name="User")
    assert user.metadata[NK.FILE_PATH] == "models.rs"


def test_rust_enum_extraction(tmp_path):
    """Rust enums are discovered as class-like nodes."""
    root = tmp_path / "repo"
    root.mkdir()
    (root / "status.rs").write_text(
        "pub enum Status {\n"
        "    Active,\n"
        "    Inactive,\n"
        "}\n"
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, _ = loader.load(str(root))

    status = _find_node(nodes, class_name="Status")
    assert status.metadata[NK.FILE_PATH] == "status.rs"


def test_rust_trait_extraction(tmp_path):
    """Rust traits are discovered as class-like nodes."""
    root = tmp_path / "repo"
    root.mkdir()
    (root / "traits.rs").write_text(
        "pub trait Repository {\n"
        "    fn find_by_id(&self, id: u64) -> Option<String>;\n"
        "}\n"
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, _ = loader.load(str(root))

    repo = _find_node(nodes, class_name="Repository")
    assert repo.metadata[NK.FILE_PATH] == "traits.rs"


# ── Tests: Grouped and aliased imports ────────────────────────────────


def test_rust_grouped_import(tmp_path):
    """Grouped use statements resolve each symbol independently."""
    root = tmp_path / "repo"
    root.mkdir()
    (root / "helpers.rs").write_text(
        "pub fn alpha() -> i32 { 1 }\n"
        "pub fn beta() -> i32 { 2 }\n"
    )
    (root / "main.rs").write_text(
        "use crate::helpers::{alpha, beta};\n"
        "\n"
        "fn run() -> i32 {\n"
        "    alpha() + beta()\n"
        "}\n"
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, edges = loader.load(str(root))

    caller = _find_node(nodes, function_name="run")
    alpha = _find_node(nodes, function_name="alpha")
    beta = _find_node(nodes, function_name="beta")

    alpha_edges = _find_edges(
        edges, from_urn=caller.urn, to_urn=alpha.urn,
        edge_type="calls",
    )
    beta_edges = _find_edges(
        edges, from_urn=caller.urn, to_urn=beta.urn,
        edge_type="calls",
    )
    assert len(alpha_edges) == 1
    assert len(beta_edges) == 1


def test_rust_aliased_import(tmp_path):
    """Aliased use statements resolve through the alias."""
    root = tmp_path / "repo"
    root.mkdir()
    (root / "helpers.rs").write_text(
        "pub fn do_work() -> i32 { 42 }\n"
    )
    (root / "main.rs").write_text(
        "use crate::helpers::do_work as work;\n"
        "\n"
        "fn run() -> i32 {\n"
        "    work()\n"
        "}\n"
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, edges = loader.load(str(root))

    caller = _find_node(nodes, function_name="run")
    callee = _find_node(nodes, function_name="do_work")

    code_to_code = _find_edges(
        edges, from_urn=caller.urn, to_urn=callee.urn,
        edge_type="calls",
    )
    assert len(code_to_code) == 1


# ── Tests: External imports don't create edges ────────────────────────


def test_rust_external_import_no_edge(tmp_path):
    """Calls to external crate functions don't produce CODE_TO_CODE edges."""
    root = tmp_path / "repo"
    root.mkdir()
    (root / "main.rs").write_text(
        "use std::fs::read_to_string;\n"
        "\n"
        "fn load_config() -> String {\n"
        '    read_to_string("config.toml").unwrap()\n'
        "}\n"
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    _, edges = loader.load(str(root))

    code_to_code = [e for e in edges if e.edge_type == "calls"]
    assert len(code_to_code) == 0


# ── Tests: impl block method discovery ────────────────────────────────


def test_rust_impl_methods_discovered(tmp_path):
    """Functions inside impl blocks are discovered as function nodes."""
    root = tmp_path / "repo"
    root.mkdir()
    (root / "vault.rs").write_text(
        "pub struct Vault {\n"
        "    name: String,\n"
        "}\n"
        "\n"
        "impl Vault {\n"
        "    pub fn open_vault(name: Option<&str>) -> Result<Vault> {\n"
        '        Vault { name: name.unwrap_or("default").to_string() }\n'
        "    }\n"
        "\n"
        "    pub fn list(&self) -> Vec<String> {\n"
        "        vec![]\n"
        "    }\n"
        "}\n"
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, _ = loader.load(str(root))

    func_names = {
        n.metadata[NK.FUNCTION_NAME]
        for n in nodes
        if NK.FUNCTION_NAME in n.metadata
    }
    assert "open_vault" in func_names
    assert "list" in func_names

    # The struct itself is also discovered
    _find_node(nodes, class_name="Vault")


# ── Tests: Qualified path calls (no use import) ──────────────────────


def test_rust_qualified_path_call(tmp_path):
    """Direct crate::path::func() calls resolve without a use import."""
    root = tmp_path / "repo"
    root.mkdir()

    svc_dir = root / "services"
    svc_dir.mkdir()
    (svc_dir / "user_svc.rs").write_text(
        "pub fn get_user_by_id(id: &str) -> String {\n"
        '    format!("User({})", id)\n'
        "}\n"
    )

    (root / "main.rs").write_text(
        "mod services;\n"
        "\n"
        "fn execute(user_id: &str) -> String {\n"
        "    crate::services::user_svc::get_user_by_id(user_id)\n"
        "}\n"
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, edges = loader.load(str(root))

    caller = _find_node(nodes, function_name="execute")
    callee = _find_node(nodes, function_name="get_user_by_id")

    code_to_code = _find_edges(
        edges,
        from_urn=caller.urn,
        to_urn=callee.urn,
        edge_type="calls",
    )
    assert len(code_to_code) == 1


def test_rust_qualified_path_to_impl_method(tmp_path):
    """crate::module::Type::method() resolves to an impl method."""
    root = tmp_path / "repo"
    root.mkdir()

    core_dir = root / "core"
    core_dir.mkdir()
    (core_dir / "vault.rs").write_text(
        "pub struct Vault;\n"
        "\n"
        "impl Vault {\n"
        "    pub fn open_vault(name: Option<&str>) -> Vault {\n"
        "        Vault\n"
        "    }\n"
        "}\n"
    )

    (root / "main.rs").write_text(
        "mod core;\n"
        "\n"
        "fn execute() {\n"
        "    crate::core::vault::Vault::open_vault(None);\n"
        "}\n"
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, edges = loader.load(str(root))

    caller = _find_node(nodes, function_name="execute")
    callee = _find_node(nodes, function_name="open_vault")

    code_to_code = _find_edges(
        edges,
        from_urn=caller.urn,
        to_urn=callee.urn,
        edge_type="calls",
    )
    assert len(code_to_code) == 1


def test_rust_bare_module_path_call(tmp_path):
    """Bare module paths like output::success() resolve if the module exists locally."""
    root = tmp_path / "repo"
    root.mkdir()
    (root / "output.rs").write_text(
        "pub fn success(msg: &str) {\n"
        '    println!("OK: {}", msg);\n'
        "}\n"
    )
    (root / "main.rs").write_text(
        "mod output;\n"
        "\n"
        "fn run() {\n"
        '    output::success("done");\n'
        "}\n"
    )

    loader = FileSystemCodebaseLoader(organization_id=ORG_ID)
    nodes, edges = loader.load(str(root))

    caller = _find_node(nodes, function_name="run")
    callee = _find_node(nodes, function_name="success")

    code_to_code = _find_edges(
        edges,
        from_urn=caller.urn,
        to_urn=callee.urn,
        edge_type="calls",
    )
    assert len(code_to_code) == 1
