"""ECR resource discovery plugin for AwsAccountLoader."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable

import boto3

from src.graph.graph_models import URN, Edge, Node
from src.graph.loaders.aws.plugins._base import AwsResourcePlugin
from src.graph.nodes.image_repository_node import ImageRepositoryNode

logger = logging.getLogger(__name__)


class EcrResourcePlugin(AwsResourcePlugin):
    """Discover ECR repositories in the account."""

    def service_name(self) -> str:
        return "ecr"

    def discover(
        self,
        session: boto3.Session,
        account_id: str,
        region: str,
        organization_id: uuid.UUID,
        account_urn: URN,
        build_urn: Callable[..., URN],
    ) -> tuple[list[Node], list[Edge]]:
        ecr = session.client("ecr", region_name=region)
        nodes: list[Node] = []
        edges: list[Edge] = []

        try:
            paginator = ecr.get_paginator("describe_repositories")
            for page in paginator.paginate():
                for repo in page.get("repositories", []):
                    repo_name = repo["repositoryName"]
                    repo_uri = repo.get("repositoryUri", "")
                    repo_arn = repo.get("repositoryArn", "")

                    ecr_urn = URN(f"urn:aws:ecr:{account_id}:{region}:{repo_name}")

                    node = ImageRepositoryNode.create(
                        organization_id=organization_id,
                        urn=ecr_urn,
                        parent_urn=account_urn,
                        repository_name=repo_name,
                        repository_uri=repo_uri,
                        arn=repo_arn,
                        account_id=account_id,
                        region=region,
                    )
                    nodes.append(node)
        except Exception:
            logger.exception("Failed to describe ECR repositories")

        return nodes, edges
