"""Stitcher and resolver registries."""

from labyrinth.graph.stitchers._base import Resolver, Stitcher
from labyrinth.graph.stitchers.apigw_to_alb import ApiGwToAlbStitcher
from labyrinth.graph.stitchers.backend_group_to_ecs import BackendGroupToEcsStitcher
from labyrinth.graph.stitchers.dns_to_load_balancer import DnsToLoadBalancerStitcher
from labyrinth.graph.stitchers.dockerfile_to_entrypoint import DockerfileToEntrypointStitcher
from labyrinth.graph.stitchers.dockerfile_to_image_repo import DockerfileToImageRepoStitcher
from labyrinth.graph.stitchers.ecs_task_to_ecr import EcsTaskToEcrStitcher
from labyrinth.graph.stitchers.function_to_table import FunctionToTableStitcher
from labyrinth.graph.stitchers.identity_center_to_iam import IdentityCenterToIamStitcher
from labyrinth.graph.stitchers.okta_to_identity_center import OktaToIdentityCenterStitcher
from labyrinth.graph.stitchers.orm_class_to_table import OrmClassToTableStitcher
from labyrinth.graph.stitchers.rds_to_database import RdsToDatabaseStitcher
from labyrinth.graph.stitchers.sg_resolver import SecurityGroupResolver
from labyrinth.graph.stitchers.sts_assume_role_relations import StsAssumeRoleRelationsStitcher

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
    IdentityCenterToIamStitcher,
    StsAssumeRoleRelationsStitcher,
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
