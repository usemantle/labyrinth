"""
GitHub codebase loader for the security graph.

URN scheme: urn:github:repo:{org}:_:{repo}/{path}
"""

from __future__ import annotations

import uuid

from src.graph.credentials import CredentialBase, NoCredential
from src.graph.graph_models import Node, NodeMetadataKey, URN
from src.graph.loaders.codebase.git_codebase_loader import GitCodebaseLoader
from src.graph.loaders.loader import URNComponent


class GithubCodebaseLoader(GitCodebaseLoader):
    """Loader for GitHub-hosted repositories."""

    def __init__(
        self,
        organization_id: uuid.UUID,
        github_org: str,
        repo_name: str,
        **kwargs,
    ):
        super().__init__(organization_id, **kwargs)
        self._github_org = github_org
        self._repo_name = repo_name

    def build_urn(self, *path_segments: str) -> URN:
        path = "/".join(path_segments)
        return URN(f"urn:github:repo:{self._github_org}:_:{path}")

    @classmethod
    def display_name(cls) -> str:
        return "GitHub Repository"

    @classmethod
    def urn_components(cls) -> list[URNComponent]:
        return [
            URNComponent("org", "GitHub organization"),
            URNComponent("repo", "Repository name"),
        ]

    @classmethod
    def credential_type(cls) -> type[CredentialBase]:
        return NoCredential

    @classmethod
    def build_target_urn(cls, **components: str) -> URN:
        return URN(f"urn:github:repo:{components['org']}:_:{components['repo']}")

    def _get_root_name(self, resource: str) -> str:
        return self._repo_name

    def _build_codebase_node(
        self,
        codebase_urn: URN,
        root_name: str,
        file_count: int,
    ) -> Node:
        node = super()._build_codebase_node(codebase_urn, root_name, file_count)
        node.metadata[NodeMetadataKey.GITHUB_ORG] = self._github_org
        return node
