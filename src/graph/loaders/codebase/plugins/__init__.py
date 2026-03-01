"""Codebase loader plugins for domain-specific node enrichment."""

from src.graph.loaders.codebase.plugins._base import CodebasePlugin
from src.graph.loaders.codebase.plugins.boto3_s3_plugin import Boto3S3Plugin
from src.graph.loaders.codebase.plugins.fastapi_plugin import FastAPIPlugin
from src.graph.loaders.codebase.plugins.sqlalchemy_plugin import SQLAlchemyPlugin

__all__ = ["Boto3S3Plugin", "CodebasePlugin", "FastAPIPlugin", "SQLAlchemyPlugin"]
