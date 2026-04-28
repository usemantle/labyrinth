"""Stitcher and resolver registries."""

from src.graph.stitchers._base import Resolver, Stitcher
from src.graph.stitchers.apigw_to_alb import ApiGwToAlbStitcher
from src.graph.stitchers.backend_group_to_ecs import BackendGroupToEcsStitcher
from src.graph.stitchers.dns_to_load_balancer import DnsToLoadBalancerStitcher
from src.graph.stitchers.dockerfile_to_entrypoint import DockerfileToEntrypointStitcher
from src.graph.stitchers.dockerfile_to_image_repo import DockerfileToImageRepoStitcher
from src.graph.stitchers.ecs_task_to_ecr import EcsTaskToEcrStitcher
from src.graph.stitchers.function_to_table import FunctionToTableStitcher
from src.graph.stitchers.okta_to_identity_center import OktaToIdentityCenterStitcher
from src.graph.stitchers.orm_class_to_table import OrmClassToTableStitcher
from src.graph.stitchers.rds_to_database import RdsToDatabaseStitcher
from src.graph.stitchers.sg_resolver import SecurityGroupResolver

STITCHER_REGISTRY: list[type[Stitcher]] = [
    OrmClassToTableStitcher,
    FunctionToTableStitcher,
    DockerfileToImageRepoStitcher,
    DockerfileToEntrypointStitcher,
    RdsToDatabaseStitcher,
    EcsTaskToEcrStitcher,
    DnsToLoadBalancerStitcher,
    ApiGwToAlbStitcher,
    BackendGroupToEcsStitcher,
    OktaToIdentityCenterStitcher,
]

RESOLVER_REGISTRY: list[type[Resolver]] = [
    SecurityGroupResolver,
]


def register_stitcher(stitcher_cls: type[Stitcher]) -> None:
    """Register an external stitcher class."""
    STITCHER_REGISTRY.append(stitcher_cls)


def register_resolver(resolver_cls: type[Resolver]) -> None:
    """Register an external resolver class."""
    RESOLVER_REGISTRY.append(resolver_cls)
