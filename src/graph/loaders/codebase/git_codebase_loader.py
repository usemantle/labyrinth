"""
Git codebase loader for the security graph.

URN scheme: urn:git:repo:{hostname}:_:{repo_path}/{file_path}

Clones or pulls any Git repository by URL (SSH or HTTPS) and scans
it using ``git ls-files`` for file enumeration.
"""

from __future__ import annotations

import logging
import subprocess
import uuid
from pathlib import Path
from urllib.parse import urlparse

from src.graph.credentials import CredentialBase, NoCredential
from src.graph.graph_models import URN, Edge, Node, NodeMetadataKey
from src.graph.loaders.codebase.codebase_loader import EXTENSION_TO_LANGUAGE, CodebaseLoader
from src.graph.loaders.loader import URNComponent

logger = logging.getLogger(__name__)


class GitCodebaseLoader(CodebaseLoader):
    """Loader for git-managed repositories accessed by URL."""

    def __init__(
        self,
        organization_id: uuid.UUID,
        repo_url: str,
        repo_hostname: str,
        repo_path: str,
        **kwargs,
    ):
        super().__init__(organization_id, **kwargs)
        self._repo_url = repo_url
        self._repo_hostname = repo_hostname
        self._repo_path = repo_path

    def build_urn(self, *path_segments: str) -> URN:
        path = "/".join(path_segments)
        return URN(f"urn:git:repo:{self._repo_hostname}:_:{path}")

    @classmethod
    def display_name(cls) -> str:
        return "Git Repository"

    @classmethod
    def urn_components(cls) -> list[URNComponent]:
        return [
            URNComponent("url", "Git clone URL (SSH or HTTPS)"),
        ]

    @classmethod
    def credential_type(cls) -> type[CredentialBase]:
        return NoCredential

    @classmethod
    def build_target_urn(cls, **components: str) -> URN:
        return URN(f"urn:git:repo:_:_:{components['url']}")

    @classmethod
    def from_target_config(
        cls, project_id: uuid.UUID, urn: URN, credentials: dict, **kwargs,
    ) -> tuple[GitCodebaseLoader, str]:
        url = urn.path
        hostname, repo_path = cls._parse_repo_url(url)
        project_dir: Path = kwargs.pop("project_dir")
        clone_path = cls._ensure_cloned(project_dir, url, hostname, repo_path)
        return cls(
            project_id,
            repo_url=url,
            repo_hostname=hostname,
            repo_path=repo_path,
            **kwargs,
        ), str(clone_path)

    # ------------------------------------------------------------------
    # Overrides
    # ------------------------------------------------------------------

    def _get_root_name(self, resource: str) -> str:
        return self._repo_path.rsplit("/", 1)[-1]

    def _build_codebase_node(
        self,
        codebase_urn: URN,
        root_name: str,
        file_count: int,
    ) -> Node:
        node = super()._build_codebase_node(codebase_urn, root_name, file_count)
        node.metadata[NodeMetadataKey.REPO_URL] = self._repo_url
        commit = self._scanned_commit
        if commit:
            node.metadata[NodeMetadataKey.SCANNED_COMMIT] = commit
        return node

    def load(self, resource: str) -> tuple[list[Node], list[Edge]]:
        self._scanned_commit = self._get_head_commit(resource)
        return super().load(resource)

    # ------------------------------------------------------------------
    # File enumeration
    # ------------------------------------------------------------------

    def _enumerate_files(self, root_path: Path) -> list[Path]:
        try:
            result = subprocess.run(
                ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
                cwd=root_path, capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0:
                files = []
                for line in sorted(result.stdout.strip().splitlines()):
                    rel = Path(line)
                    if any(part in self._exclude_dirs for part in rel.parts):
                        continue
                    if rel.suffix in EXTENSION_TO_LANGUAGE:
                        full = root_path / rel
                        if full.is_file():
                            files.append(full)
                return files
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        # Fallback to rglob
        files: list[Path] = []
        for path in sorted(root_path.rglob("*")):
            if not path.is_file():
                continue
            if any(part in self._exclude_dirs for part in path.relative_to(root_path).parts):
                continue
            if path.suffix in EXTENSION_TO_LANGUAGE:
                files.append(path)
        return files

    # ------------------------------------------------------------------
    # Git helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_head_commit(repo_path: str) -> str | None:
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repo_path, capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        logger.warning("Could not determine HEAD commit for %s", repo_path)
        return None

    @staticmethod
    def _parse_repo_url(url: str) -> tuple[str, str]:
        """Extract (hostname, path) from a git URL.

        Handles both HTTPS (https://github.com/acme/repo.git)
        and SSH (git@github.com:acme/repo.git) formats.
        """
        if url.startswith("git@") or (not url.startswith("http") and ":" in url):
            # SSH format: git@github.com:acme/repo.git
            host_part, _, path_part = url.partition(":")
            hostname = host_part.split("@", 1)[-1]
            repo_path = path_part.removesuffix(".git").strip("/")
        else:
            parsed = urlparse(url)
            hostname = parsed.hostname or "unknown"
            repo_path = parsed.path.removesuffix(".git").strip("/")
        return hostname, repo_path

    @staticmethod
    def _ensure_cloned(
        project_dir: Path,
        url: str,
        hostname: str,
        repo_path: str,
    ) -> Path:
        """Clone or pull a git repo into the project's repos directory."""
        repos_dir = project_dir / "repos" / hostname / Path(repo_path).parent
        repos_dir.mkdir(parents=True, exist_ok=True)
        repo_local = project_dir / "repos" / hostname / repo_path

        if repo_local.exists():
            logger.info("Pulling latest for %s...", url)
            subprocess.run(
                ["git", "pull"],
                cwd=repo_local, capture_output=True, check=False,
            )
        else:
            logger.info("Cloning %s...", url)
            subprocess.run(
                ["git", "clone", url, str(repo_local)],
                capture_output=True, check=True,
            )

        return repo_local
