"""Codebase loader plugins for domain-specific node enrichment."""

from src.graph.loaders.codebase.plugins._base import CodebasePlugin
from src.graph.loaders.codebase.plugins.boto3_s3_plugin import Boto3S3Plugin
from src.graph.loaders.codebase.plugins.fastapi_plugin import FastAPIPlugin
from src.graph.loaders.codebase.plugins.flask_plugin import FlaskPlugin
from src.graph.loaders.codebase.plugins.requests_plugin import RequestsPlugin
from src.graph.loaders.codebase.plugins.sqlalchemy_plugin import SQLAlchemyPlugin
from src.graph.loaders.codebase.plugins.uv_plugin import UvPlugin

__all__ = [
    "Boto3S3Plugin",
    "CodebasePlugin",
    "FastAPIPlugin",
    "FlaskPlugin",
    "RequestsPlugin",
    "SQLAlchemyPlugin",
    "UvPlugin",
]
