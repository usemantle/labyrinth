"""
Unit tests for GitCodebaseLoader.

All filesystem access is on real tmp directories; git operations are mocked.
"""

import uuid
from pathlib import Path
from unittest.mock import patch

import pytest

from labyrinth.graph.graph_models import (
    NodeMetadataKey,
)
from labyrinth.graph.loaders.codebase.git_codebase_loader import GitCodebaseLoader

ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")

NK = NodeMetadataKey


# ── Helpers ────────────────────────────────────────────────────────────


def _make_tmp_repo(tmp_path: Path) -> Path:
    """Create a minimal fake repo on disk."""
    repo = tmp_path / "my-repo"
    repo.mkdir()

    # Python file with a class and top-level function
    (repo / "app.py").write_text(
        'class UserService:\n'
        '    def get_user(self, user_id):\n'
        '        return {"id": user_id}\n'
        '\n'
        'def health_check():\n'
        '    return "ok"\n'
    )

    # Python file with decorated class
    models = repo / "models"
    models.mkdir()
    (models / "__init__.py").write_text("")
    (models / "user.py").write_text(
        'from dataclasses import dataclass\n'
        '\n'
        '@dataclass\n'
        'class User:\n'
        '    email: str\n'
        '    name: str\n'
    )

    # JavaScript file
    (repo / "index.js").write_text(
        'class App {\n'
        '    start() {\n'
        '        console.log("started");\n'
        '    }\n'
        '}\n'
        '\n'
        'function main() {\n'
        '    new App().start();\n'
        '}\n'
    )

    # Empty Python file (should still get a file node)
    (repo / "empty.py").write_text("")

    # Nested class
    (repo / "nested.py").write_text(
        'class Outer:\n'
        '    class Inner:\n'
        '        def inner_method(self):\n'
        '            pass\n'
        '\n'
        '    def outer_method(self):\n'
        '        pass\n'
    )

    # Should be excluded: node_modules
    nm = repo / "node_modules" / "pkg"
    nm.mkdir(parents=True)
    (nm / "index.js").write_text('function hidden() {}')

    # Non-code file (should be excluded)
    (repo / "README.md").write_text("# My Repo")

    return repo


# ── GitCodebaseLoader tests ─────────────────────────────────────────


@pytest.fixture()
def git_result(tmp_path):
    """Run GitCodebaseLoader on a temporary repo."""
    repo = _make_tmp_repo(tmp_path)
    with patch(
        "labyrinth.graph.loaders.codebase.git_codebase_loader.GitCodebaseLoader._get_head_commit"
    ) as mock_commit:
        mock_commit.return_value = "abc123def456"
        loader = GitCodebaseLoader(
            organization_id=ORG_ID,
            repo_url="https://github.com/acme/my-service.git",
            repo_hostname="github.com",
            repo_path="acme/my-service",
        )
        return loader.load(str(repo))


def test_git_urn_scheme(git_result):
    nodes, _ = git_result
    root = nodes[0]
    assert str(root.urn) == "urn:git:repo:github.com:_:my-service"


def test_git_root_metadata(git_result):
    nodes, _ = git_result
    root = nodes[0]
    assert root.metadata[NK.REPO_NAME] == "my-service"
    assert root.metadata[NK.SCANNED_COMMIT] == "abc123def456"
    assert root.metadata[NK.REPO_URL] == "https://github.com/acme/my-service.git"


def test_git_file_urn(git_result):
    nodes, _ = git_result
    app = next(
        n for n in nodes
        if n.metadata.get(NK.FILE_PATH) == "app.py" and NK.CLASS_NAME not in n.metadata
    )
    assert str(app.urn) == "urn:git:repo:github.com:_:my-service/app.py"


def test_git_class_urn(git_result):
    nodes, _ = git_result
    user_service = next(n for n in nodes if n.metadata.get(NK.CLASS_NAME) == "UserService")
    assert str(user_service.urn) == "urn:git:repo:github.com:_:my-service/app.py/UserService"


def test_git_method_urn(git_result):
    nodes, _ = git_result
    get_user = next(n for n in nodes if n.metadata.get(NK.FUNCTION_NAME) == "get_user")
    assert str(get_user.urn) == "urn:git:repo:github.com:_:my-service/app.py/UserService/get_user"


def test_git_uses_repo_name_not_dir_name(git_result):
    """Root name is repo_path last segment, not the tmp dir name."""
    nodes, _ = git_result
    root = nodes[0]
    assert "my-service" in str(root.urn)
    assert root.metadata[NK.REPO_NAME] == "my-service"
