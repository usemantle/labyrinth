"""
Abstract git-aware codebase loader.

Adds scanned_commit tracking to the codebase root node metadata.
Concrete subclasses (GithubCodebaseLoader, BitbucketCodebaseLoader)
provide URN construction and provider-specific metadata.
"""

import abc
import logging
import subprocess
from pathlib import Path

from src.graph.graph_models import Node, NodeMetadataKey, URN
from src.graph.loaders.codebase.codebase_loader import CodebaseLoader

logger = logging.getLogger(__name__)


class GitCodebaseLoader(CodebaseLoader, abc.ABC):
    """Abstract loader for git-managed codebases.

    Enriches the codebase root node with ``scanned_commit`` (the HEAD
    SHA at scan time).  A future ``load_incremental()`` method will use
    this to diff-scan only changed files.
    """

    def _build_codebase_node(
        self,
        codebase_urn: URN,
        root_name: str,
        file_count: int,
    ) -> Node:
        node = super()._build_codebase_node(codebase_urn, root_name, file_count)
        commit = self._scanned_commit
        if commit:
            node.metadata[NodeMetadataKey.SCANNED_COMMIT] = commit
        return node

    def load(self, resource: str) -> tuple[list[Node], list["Edge"]]:  # noqa: F821
        """Full scan of the repo at *resource* path."""
        self._scanned_commit = self._get_head_commit(resource)
        return super().load(resource)

    @staticmethod
    def _get_head_commit(repo_path: str) -> str | None:
        """Return the HEAD commit SHA, or None if not a git repo."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repo_path,
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        logger.warning("Could not determine HEAD commit for %s", repo_path)
        return None
