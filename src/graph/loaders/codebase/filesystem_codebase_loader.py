"""
Local filesystem codebase loader for the security graph.

URN scheme: urn:local:codebase:{hostname}:_:{path}

For scanning directories without git context (e.g. mounted volumes,
extracted archives, or local development directories).
"""

from __future__ import annotations

import uuid
from pathlib import Path

from src.graph.credentials import CredentialBase, NoCredential
from src.graph.graph_models import URN
from src.graph.loaders.codebase.codebase_loader import EXTENSION_TO_LANGUAGE, CodebaseLoader
from src.graph.loaders.loader import URNComponent


class FileSystemCodebaseLoader(CodebaseLoader):
    """Loader for local filesystem directories."""

    def __init__(
        self,
        organization_id: uuid.UUID,
        hostname: str = "localhost",
        **kwargs,
    ):
        super().__init__(organization_id, **kwargs)
        self._hostname = hostname

    def build_urn(self, *path_segments: str) -> URN:
        path = "/".join(path_segments)
        return URN(f"urn:local:codebase:{self._hostname}:_:{path}")

    @classmethod
    def display_name(cls) -> str:
        return "Local Codebase"

    @classmethod
    def urn_components(cls) -> list[URNComponent]:
        return [
            URNComponent("path", "Path to codebase root"),
        ]

    @classmethod
    def credential_type(cls) -> type[CredentialBase]:
        return NoCredential

    @classmethod
    def build_target_urn(cls, **components: str) -> URN:
        return URN(f"urn:_:repo:_:_:{components['path']}")

    @classmethod
    def from_target_config(
        cls, project_id: uuid.UUID, urn: URN, credentials: dict, **kwargs,
    ) -> tuple[FileSystemCodebaseLoader, str]:
        kwargs.pop("project_dir", None)
        resolved = str(Path(urn.path).expanduser().resolve())
        return cls(project_id, hostname=resolved, **kwargs), urn.path

    def _enumerate_files(self, root_path: Path) -> list[Path]:
        files: list[Path] = []
        for path in sorted(root_path.rglob("*")):
            if not path.is_file():
                continue
            if any(part in self._exclude_dirs for part in path.relative_to(root_path).parts):
                continue
            if path.suffix in EXTENSION_TO_LANGUAGE:
                files.append(path)
        return files
