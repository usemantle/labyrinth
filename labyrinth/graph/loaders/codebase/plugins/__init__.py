"""Codebase loader plugins for domain-specific node enrichment."""

from labyrinth.graph.loaders.codebase.plugins._base import CodebasePlugin
from labyrinth.graph.loaders.codebase.plugins.boto3_s3_plugin import Boto3S3Plugin
from labyrinth.graph.loaders.codebase.plugins.fastapi_plugin import FastAPIPlugin
from labyrinth.graph.loaders.codebase.plugins.flask_plugin import FlaskPlugin
from labyrinth.graph.loaders.codebase.plugins.requests_plugin import RequestsPlugin
from labyrinth.graph.loaders.codebase.plugins.sqlalchemy_plugin import SQLAlchemyPlugin
from labyrinth.graph.loaders.codebase.plugins.uv_plugin import UvPlugin

__all__ = [
    "Boto3S3Plugin",
    "CodebasePlugin",
    "FastAPIPlugin",
    "FlaskPlugin",
    "RequestsPlugin",
    "SQLAlchemyPlugin",
    "UvPlugin",
]
