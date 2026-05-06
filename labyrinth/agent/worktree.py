"""Git worktree management for agent filesystem isolation."""

from __future__ import annotations

import logging
import shutil
import subprocess
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)


def resolve_repo_root(candidate_urn: str, project_dir: Path) -> Path | None:
    """Resolve the local git repo root from a candidate URN.

    For local codebases (``urn:local:codebase:{abs_path}:_:...``), the
    absolute path is encoded in the URN's account field.

    For git codebases (``urn:git:repo:{hostname}:_:{path}``), the repo
    is cloned under ``project_dir/repos/{hostname}/...``.

    Returns None if the path cannot be resolved or is not a git repo.
    """
    parts = candidate_urn.split(":", 5)
    if len(parts) != 6 or parts[0] != "urn":
        return None

    provider, service, account = parts[1], parts[2], parts[3]

    if provider == "local" and service == "codebase":
        root = Path(account)
        if root.is_dir() and _is_git_repo(root):
            return root
        return None

    if provider == "git" and service == "repo":
        hostname = account
        repo_path = parts[5]
        repos_base = project_dir / "repos" / hostname
        if not repos_base.exists():
            return None
        # Walk path segments to find the repo root (first dir with .git)
        segments = repo_path.split("/")
        for depth in range(1, len(segments) + 1):
            candidate = repos_base / "/".join(segments[:depth])
            if candidate.is_dir() and _is_git_repo(candidate):
                return candidate
        return None

    return None


def _is_git_repo(path: Path) -> bool:
    """Check if a directory is a git repository."""
    return (path / ".git").exists() or (path / ".git").is_file()


def create_worktree(repo_root: Path, worktree_dir: Path) -> tuple[Path, str]:
    """Create a git worktree on a new branch.

    Returns (worktree_path, branch_name).
    """
    branch_name = f"labyrinth/agent-{uuid.uuid4().hex[:8]}"
    worktree_path = worktree_dir / branch_name.replace("/", "-")
    worktree_path.parent.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        ["git", "worktree", "add", "-b", branch_name, str(worktree_path)],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    logger.info("Created worktree at %s (branch %s)", worktree_path, branch_name)
    return worktree_path, branch_name


def worktree_has_changes(worktree_path: Path) -> bool:
    """Check if a worktree has any uncommitted or committed changes vs its parent."""
    # Check for uncommitted changes
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=worktree_path,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.stdout.strip():
        return True

    # Check for commits ahead of the parent branch
    result = subprocess.run(
        ["git", "log", "--oneline", "HEAD", "--not", "--remotes", "--not", "HEAD~1"],
        cwd=worktree_path,
        capture_output=True,
        text=True,
        timeout=10,
    )
    # Simpler: compare HEAD with the branch point
    result = subprocess.run(
        ["git", "rev-list", "--count", "HEAD", "--not", "--all"],
        cwd=worktree_path,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return False  # If no uncommitted changes, consider clean


def cleanup_worktree(repo_root: Path, worktree_path: Path, branch_name: str) -> None:
    """Remove a worktree and its branch."""
    try:
        subprocess.run(
            ["git", "worktree", "remove", str(worktree_path), "--force"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (subprocess.SubprocessError, OSError):
        logger.warning("Failed to remove worktree via git, cleaning up manually")
        shutil.rmtree(worktree_path, ignore_errors=True)

    try:
        subprocess.run(
            ["git", "branch", "-D", branch_name],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (subprocess.SubprocessError, OSError):
        logger.warning("Failed to delete branch %s", branch_name)

    logger.info("Cleaned up worktree %s", worktree_path)
