from __future__ import annotations

import abc
import uuid
from dataclasses import dataclass

from labyrinth.graph.credentials import CredentialBase
from labyrinth.graph.graph_models import URN, Edge, Node


@dataclass
class URNComponent:
    """A variable part of a URN that the user must provide when registering a target."""

    name: str
    description: str
    default: str | None = None


class ConceptLoader(abc.ABC):
    """
    Abstract interface for loading nodes and edges into the security graph.

    Each concrete loader discovers resources from a specific source type
    (Postgres, DynamoDB, S3, GitHub, IAM, etc.) and transforms them into
    graph nodes and edges.

    The organization_id is set at construction — one loader instance per
    tenant invocation.
    """

    def __init__(self, organization_id: uuid.UUID):
        self.organization_id = organization_id

    @abc.abstractmethod
    def build_urn(self, *path_segments: str) -> URN:
        """Construct a URN from path segments. Each loader defines its own scheme."""
        ...

    @abc.abstractmethod
    def load(self, resource: str) -> tuple[list[Node], list[Edge]]:
        """
        Discover nodes and edges from the given resource.

        Args:
            resource: A resource-specific connection string or identifier.
                      For Postgres: a connection string (postgresql://...).
                      For S3: a bucket ARN. For GitHub: an org/repo path.

        Returns:
            A tuple of (nodes, edges) discovered from the resource.
        """
        ...

    @classmethod
    @abc.abstractmethod
    def display_name(cls) -> str:
        """Human-readable name shown in the target selector."""
        ...

    @classmethod
    @abc.abstractmethod
    def urn_components(cls) -> list[URNComponent]:
        """Variable URN parts the user must provide to identify a target."""
        ...

    @classmethod
    @abc.abstractmethod
    def credential_type(cls) -> type[CredentialBase]:
        """The credential type this loader requires."""
        ...

    @classmethod
    @abc.abstractmethod
    def build_target_urn(cls, **components: str) -> URN:
        """Build the root target URN from the provided component values."""
        ...

    @classmethod
    def available_plugins(cls) -> dict[str, type]:
        """Plugin-name -> plugin-class for this loader. Empty for non-codebase loaders."""
        return {}

    @classmethod
    @abc.abstractmethod
    def from_target_config(
        cls, project_id: uuid.UUID, urn: URN, credentials: dict, **kwargs,
    ) -> tuple[ConceptLoader, str]:
        """Create a loader and resource string from stored target configuration.

        Args:
            project_id: The project UUID used as organization_id.
            urn: The parsed target URN from the project config.
            credentials: Credential dict from the project config.
            **kwargs: Loader-specific extra arguments (e.g. clone_path).

        Returns:
            A tuple of (loader_instance, resource_string_for_load).
        """
        ...
