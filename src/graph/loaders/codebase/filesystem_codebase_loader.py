"""
Local filesystem codebase loader for the security graph.

URN scheme: urn:local:codebase:{hostname}:_:{path}

For scanning directories without git context (e.g. mounted volumes,
extracted archives, or local development directories).
"""

import uuid

from src.graph.graph_models import URN
from src.graph.loaders.codebase.codebase_loader import CodebaseLoader


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
