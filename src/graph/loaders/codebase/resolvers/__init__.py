"""Core language analyzers for cross-file resolution and call graph extraction."""

from src.graph.loaders.codebase.resolvers._base import (
    CallSite,
    LanguageAnalyzer,
    ResolvedImport,
)
from src.graph.loaders.codebase.resolvers.python import PythonAnalyzer

LANGUAGE_ANALYZERS: dict[str, LanguageAnalyzer] = {
    "python": PythonAnalyzer(),
}

__all__ = [
    "CallSite",
    "LANGUAGE_ANALYZERS",
    "LanguageAnalyzer",
    "PythonAnalyzer",
    "ResolvedImport",
]
