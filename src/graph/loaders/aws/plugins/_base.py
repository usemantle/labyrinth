"""Abstract base class for AWS resource discovery plugins."""

from __future__ import annotations

import abc
import uuid
from collections.abc import Callable

import boto3

from src.graph.graph_models import URN, Edge, Node


class AwsResourcePlugin(abc.ABC):
    """Base class for AWS service-specific resource discovery.

    Each plugin discovers resources for a single AWS service and returns
    the nodes and edges it found.  The parent ``AwsAccountLoader`` calls
    ``discover()`` for every enabled plugin and merges the results.
    """

    @abc.abstractmethod
    def service_name(self) -> str:
        """Short identifier used in TOML plugin lists (e.g. ``"s3"``)."""
        ...

    @abc.abstractmethod
    def discover(
        self,
        session: boto3.Session,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        build_urn: Callable[..., URN],
    ) -> tuple[list[Node], list[Edge]]:
        """Discover resources for this service.

        Args:
            session: Authenticated boto3 session for this account/region.
            account_id: The AWS account ID.
            region: The AWS region being scanned.
            organization_id: Tenant/project UUID.
            account_urn: URN of the parent ``AwsAccountNode``.
            build_urn: Helper to construct URNs within the account scope.

        Returns:
            A tuple of (nodes, edges) discovered from the service.
        """
        ...
