"""Core language analyzers for cross-file resolution and call graph extraction."""

from src.graph.loaders.codebase.resolvers._base import (
    CallSite,
    LanguageAnalyzer,
    ResolvedImport,
)
from src.graph.loaders.codebase.resolvers.python import PythonAnalyzer
from src.graph.loaders.codebase.resolvers.rust import RustAnalyzer

LANGUAGE_ANALYZERS: dict[str, LanguageAnalyzer] = {
    "python": PythonAnalyzer(),
    "rust": RustAnalyzer(),
}

__all__ = [
    "CallSite",
    "LANGUAGE_ANALYZERS",
    "LanguageAnalyzer",
    "PythonAnalyzer",
    "ResolvedImport",
    "RustAnalyzer",
]
