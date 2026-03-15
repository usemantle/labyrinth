"""AWS resource discovery plugins."""

from src.graph.loaders.aws.plugins._base import AwsResourcePlugin
from src.graph.loaders.aws.plugins.ecr_plugin import EcrResourcePlugin
from src.graph.loaders.aws.plugins.ecs_plugin import EcsResourcePlugin
from src.graph.loaders.aws.plugins.iam_plugin import IamResourcePlugin
from src.graph.loaders.aws.plugins.rds_plugin import RdsResourcePlugin
from src.graph.loaders.aws.plugins.s3_plugin import S3ResourcePlugin
from src.graph.loaders.aws.plugins.sso_plugin import SsoResourcePlugin
from src.graph.loaders.aws.plugins.vpc_plugin import VpcResourcePlugin

__all__ = [
    "AwsResourcePlugin",
    "EcrResourcePlugin",
    "EcsResourcePlugin",
    "IamResourcePlugin",
    "RdsResourcePlugin",
    "S3ResourcePlugin",
    "SsoResourcePlugin",
    "VpcResourcePlugin",
]
