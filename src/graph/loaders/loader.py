import abc
import uuid

from src.graph.graph_models import Node, Edge, URN


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
