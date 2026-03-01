"""
GitHub organization loader for the security graph.

Discovers repositories within a GitHub organization and produces
org + repo nodes with CONTAINS edges. Repo node URNs match the
GithubCodebaseLoader root node URN scheme, enabling automatic
graph merge via URN deduplication.

URN schemes:
    Org:  urn:github:org:{org}:_:{org}
    Repo: urn:github:repo:{org}:_:{repo_name}
"""

from __future__ import annotations

import json
import logging
import urllib.request
import uuid

from src.graph.credentials import CredentialBase, GithubTokenCredential
from src.graph.graph_models import Edge, Node, NodeMetadata, NodeMetadataKey, RelationType, URN
from src.graph.loaders._helpers import make_edge
from src.graph.loaders.loader import ConceptLoader, URNComponent

logger = logging.getLogger(__name__)


class GithubOrgLoader(ConceptLoader):
    """Loader that discovers repositories in a GitHub organization."""

    def __init__(
        self,
        organization_id: uuid.UUID,
        github_org: str,
        github_token: str,
    ):
        super().__init__(organization_id)
        self._github_org = github_org
        self._github_token = github_token

    def build_urn(self, *path_segments: str) -> URN:
        path = "/".join(path_segments)
        return URN(f"urn:github:org:{self._github_org}:_:{path}")

    @classmethod
    def display_name(cls) -> str:
        return "GitHub Organization"

    @classmethod
    def urn_components(cls) -> list[URNComponent]:
        return [
            URNComponent("org", "GitHub organization name"),
        ]

    @classmethod
    def credential_type(cls) -> type[CredentialBase]:
        return GithubTokenCredential

    @classmethod
    def build_target_urn(cls, **components: str) -> URN:
        org = components["org"]
        return URN(f"urn:github:org:{org}:_:{org}")

    def load(self, resource: str) -> tuple[list[Node], list[Edge]]:
        """Discover org structure and repositories.

        Args:
            resource: GitHub org name (typically same as constructor arg,
                      kept for ConceptLoader interface compatibility).

        Returns:
            Org node + repo nodes with CONTAINS edges.
        """
        repos = self._fetch_repos()

        nodes: list[Node] = []
        edges: list[Edge] = []

        # Organization root node
        org_urn = self.build_urn(self._github_org)
        nodes.append(Node(
            organization_id=self.organization_id,
            urn=org_urn,
            parent_urn=None,
            metadata=NodeMetadata({
                NodeMetadataKey.ORG_NAME: self._github_org,
                NodeMetadataKey.REPO_COUNT: len(repos),
            }),
        ))

        # One node per repository
        for repo in repos:
            repo_name = repo["name"]
            # Repo URN uses the codebase loader scheme for auto-merge
            repo_urn = URN(f"urn:github:repo:{self._github_org}:_:{repo_name}")

            nodes.append(Node(
                organization_id=self.organization_id,
                urn=repo_urn,
                parent_urn=org_urn,
                metadata=NodeMetadata({
                    NodeMetadataKey.REPO_NAME: repo_name,
                    NodeMetadataKey.FULL_NAME: repo.get("full_name", f"{self._github_org}/{repo_name}"),
                    NodeMetadataKey.PRIVATE: repo.get("private", False),
                    NodeMetadataKey.DEFAULT_BRANCH: repo.get("default_branch", "main"),
                    NodeMetadataKey.LANGUAGE: repo.get("language"),
                    NodeMetadataKey.ARCHIVED: repo.get("archived", False),
                    NodeMetadataKey.CLONE_URL: repo.get("clone_url"),
                }),
            ))
            edges.append(make_edge(
                self.organization_id, org_urn, repo_urn, RelationType.CONTAINS,
            ))

        logger.info(
            "Discovered %d repositories in GitHub org %s",
            len(repos), self._github_org,
        )

        return nodes, edges

    def _fetch_repos(self) -> list[dict]:
        """Fetch all repositories from the GitHub API (paginated)."""
        repos: list[dict] = []
        page = 1

        while True:
            url = (
                f"https://api.github.com/orgs/{self._github_org}/repos"
                f"?per_page=100&page={page}&type=all"
            )
            req = urllib.request.Request(
                url,
                headers={
                    "Authorization": f"Bearer {self._github_token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )

            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = json.loads(resp.read().decode())
            except Exception:
                logger.exception("Failed to fetch repos page %d for %s", page, self._github_org)
                break

            if not data:
                break

            repos.extend(data)
            if len(data) < 100:
                break
            page += 1

        return repos
