"""Mixin for AWS resource nodes — URNs are constructed as ``urn:<arn>``.

AWS Amazon Resource Names (ARNs) are globally unique within AWS; piggy-backing
on them for our URN scheme guarantees node-identity uniqueness across accounts,
regions, and services without inventing a parallel namespace.

For resources that AWS doesn't expose a real ARN for (e.g. Route53 record sets,
identity-store users), nodes synthesise an ARN-shaped string with the same
``arn:partition:service:region:account:resource`` layout so the URN scheme
stays uniform.
"""

from __future__ import annotations

from labyrinth.graph.graph_models import URN


class AwsResourceMixin:
    """Common URN construction for AWS resource nodes."""

    @staticmethod
    def urn_from_arn(arn: str) -> URN:
        """Wrap an ARN as the URN string ``urn:<arn>``."""
        return URN(f"urn:{arn}")
