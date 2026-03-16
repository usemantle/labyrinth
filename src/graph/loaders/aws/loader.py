"""AwsAccountLoader — discovers AWS resources across services via plugins."""

from __future__ import annotations

import logging
import uuid

import boto3

from src.graph.credentials import AWSProfileCredential, CredentialBase
from src.graph.edges.contains_edge import ContainsEdge
from src.graph.graph_models import URN, Edge, Node
from src.graph.loaders.aws.plugins._base import AwsResourcePlugin
from src.graph.loaders.aws.plugins.apigateway_plugin import ApiGatewayResourcePlugin
from src.graph.loaders.aws.plugins.ecr_plugin import EcrResourcePlugin
from src.graph.loaders.aws.plugins.ecs_plugin import EcsResourcePlugin
from src.graph.loaders.aws.plugins.elbv2_plugin import Elbv2ResourcePlugin
from src.graph.loaders.aws.plugins.iam_plugin import IamResourcePlugin
from src.graph.loaders.aws.plugins.rds_plugin import RdsResourcePlugin
from src.graph.loaders.aws.plugins.route53_plugin import Route53ResourcePlugin
from src.graph.loaders.aws.plugins.s3_plugin import S3ResourcePlugin
from src.graph.loaders.aws.plugins.sso_plugin import SsoResourcePlugin
from src.graph.loaders.aws.plugins.vpc_plugin import VpcResourcePlugin
from src.graph.loaders.loader import ConceptLoader, URNComponent
from src.graph.nodes.aws_account_node import AwsAccountNode

logger = logging.getLogger(__name__)

_ALL_PLUGINS: dict[str, type[AwsResourcePlugin]] = {
    "s3": S3ResourcePlugin,
    "rds": RdsResourcePlugin,
    "ecr": EcrResourcePlugin,
    "ecs": EcsResourcePlugin,
    "vpc": VpcResourcePlugin,
    "iam": IamResourcePlugin,
    "sso": SsoResourcePlugin,
    "route53": Route53ResourcePlugin,
    "elbv2": Elbv2ResourcePlugin,
    "apigateway": ApiGatewayResourcePlugin,
}


class AwsAccountLoader(ConceptLoader):
    """Discover AWS resources for a single account/region via plugins.

    URN scheme: ``urn:aws:account:{account_id}:{region}:root``
    """

    def __init__(
        self,
        organization_id: uuid.UUID,
        account_id: str,
        region: str,
        session: boto3.Session,
        plugins: list[AwsResourcePlugin] | None = None,
    ):
        super().__init__(organization_id)
        self._account_id = account_id
        self._region = region
        self._session = session
        self._plugins = plugins or []

    def build_urn(self, *path_segments: str) -> URN:
        path = "/".join(path_segments)
        return URN(f"urn:aws:account:{self._account_id}:{self._region}:{path}")

    def load(self, resource: str) -> tuple[list[Node], list[Edge]]:
        account_urn = URN(f"urn:aws:account:{self._account_id}:{self._region}:root")
        account_node = AwsAccountNode.create(
            organization_id=self.organization_id,
            urn=account_urn,
            account_id=self._account_id,
            region=self._region,
        )

        all_nodes: list[Node] = [account_node]
        all_edges: list[Edge] = []

        for plugin in self._plugins:
            logger.info("  Running %s plugin...", plugin.service_name())
            try:
                nodes, edges = plugin.discover(
                    session=self._session,
                    account_id=self._account_id,
                    region=self._region,
                    organization_id=self.organization_id,
                    account_urn=account_urn,
                    build_urn=self.build_urn,
                )
                # Create ContainsEdge from account to each top-level resource node
                for node in nodes:
                    if node.parent_urn == account_urn:
                        all_edges.append(ContainsEdge.create(
                            self.organization_id, account_urn, node.urn,
                        ))
                all_nodes.extend(nodes)
                all_edges.extend(edges)
                logger.info(
                    "    %s: %d nodes, %d edges",
                    plugin.service_name(), len(nodes), len(edges),
                )
            except Exception:
                logger.exception("Plugin %s failed", plugin.service_name())

        return all_nodes, all_edges

    @classmethod
    def display_name(cls) -> str:
        return "AWS Account"

    @classmethod
    def urn_components(cls) -> list[URNComponent]:
        return [
            URNComponent("account_id", "AWS account ID (e.g. 123456789012)"),
            URNComponent("region", "AWS region (e.g. us-east-1)", default="us-east-1"),
        ]

    @classmethod
    def credential_type(cls) -> type[CredentialBase]:
        return AWSProfileCredential

    @classmethod
    def build_target_urn(cls, **components: str) -> URN:
        account_id = components["account_id"]
        region = components.get("region", "us-east-1")
        return URN(f"urn:aws:account:{account_id}:{region}:root")

    @classmethod
    def available_plugins(cls) -> dict[str, type]:
        return dict(_ALL_PLUGINS)

    @classmethod
    def from_target_config(
        cls, project_id: uuid.UUID, urn: URN, credentials: dict, **kwargs,
    ) -> tuple[AwsAccountLoader, str]:
        session = boto3.Session(
            profile_name=credentials.get("profile", "default"),
            region_name=urn.region,
        )

        # Resolve account ID — use URN or STS
        account_id = urn.account
        sts = session.client("sts")
        resolved_account = sts.get_caller_identity()["Account"]

        if not account_id or account_id == "x":
            account_id = resolved_account
        elif resolved_account != account_id:
            raise ValueError(
                f"AWS profile resolves to account {resolved_account} but target "
                f"URN specifies {account_id}. Check your AWS profile configuration."
            )

        # Instantiate plugins from kwargs if provided by scan.py
        plugins: list[AwsResourcePlugin] = kwargs.get("plugins", [])

        loader = cls(
            organization_id=project_id,
            account_id=account_id,
            region=urn.region,
            session=session,
            plugins=plugins,
        )
        return loader, urn.path
