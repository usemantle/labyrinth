"""CVE data sourcing for dependency vulnerability scanning."""

from labyrinth.graph.loaders.codebase.cve.osv_client import OsvResult, query_osv

__all__ = ["OsvResult", "query_osv"]
