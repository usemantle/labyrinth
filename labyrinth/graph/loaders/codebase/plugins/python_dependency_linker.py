"""Python dependency linker — connects Python files to dependency nodes.

Extracts ``import X`` and ``from X import ...`` statements, then resolves
package names to import names using the target project's installed package
metadata (``top_level.txt`` or ``RECORD`` in dist-info). Falls back to
standard PyPI normalization (replace hyphens with underscores, lowercase)
when metadata is unavailable.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

from labyrinth.graph.loaders.codebase.plugins.dependency_linker import DependencyLinkerPlugin

if TYPE_CHECKING:
    from labyrinth.graph.loaders.codebase.codebase_loader import PostProcessContext

logger = logging.getLogger(__name__)

_IMPORT_RE = re.compile(r"^import\s+(\w+)", re.MULTILINE)
_FROM_IMPORT_RE = re.compile(r"^from\s+(\w+)", re.MULTILINE)


def _find_site_packages(root_path: Path) -> Path | None:
    """Locate site-packages in the project's virtualenv."""
    for venv_name in (".venv", "venv"):
        venv = root_path / venv_name
        if not venv.is_dir():
            continue
        # Linux/macOS: lib/pythonX.Y/site-packages
        for sp in (venv / "lib").glob("python*/site-packages"):
            if sp.is_dir():
                return sp
        # Windows: Lib/site-packages
        sp = venv / "Lib" / "site-packages"
        if sp.is_dir():
            return sp
    return None


def _build_import_map(site_packages: Path) -> dict[str, set[str]]:
    """Build {normalized_dist_name: {import_names}} from dist-info metadata.

    Tries ``top_level.txt`` first, then falls back to parsing ``RECORD``
    for top-level packages/modules.
    """
    result: dict[str, set[str]] = {}

    for dist_info in site_packages.glob("*.dist-info"):
        # Extract distribution name from directory name (name-version.dist-info)
        dist_dir_name = dist_info.name
        # Remove .dist-info suffix, then split off version
        name_version = dist_dir_name.removesuffix(".dist-info")
        # dist-info names use _ for - per PEP 427
        parts = name_version.split("-", 1)
        dist_name = parts[0].lower().replace("_", "-") if parts else ""
        if not dist_name:
            continue

        imports = _read_top_level_txt(dist_info)
        if not imports:
            imports = _parse_record(dist_info)
        if imports:
            result[dist_name] = imports

    return result


def _read_top_level_txt(dist_info: Path) -> set[str]:
    """Read import names from top_level.txt."""
    top_level = dist_info / "top_level.txt"
    if not top_level.exists():
        return set()
    try:
        return {
            line.strip().lower()
            for line in top_level.read_text().splitlines()
            if line.strip() and not line.strip().startswith("_")
        }
    except OSError:
        return set()


def _parse_record(dist_info: Path) -> set[str]:
    """Extract top-level package names from RECORD file."""
    record = dist_info / "RECORD"
    if not record.exists():
        return set()
    try:
        top_level: set[str] = set()
        dist_info_name = dist_info.name
        for line in record.read_text().splitlines():
            path = line.split(",")[0].strip()
            if not path or path.startswith(".."):
                continue
            if "/" in path:
                first = path.split("/")[0]
                if (
                    not first.endswith(".dist-info")
                    and not first.startswith("__")
                    and not first.startswith("_")
                    and first != dist_info_name
                ):
                    top_level.add(first.lower())
            elif path.endswith(".py") and not path.startswith("_"):
                top_level.add(path[:-3].lower())
        return top_level
    except OSError:
        return set()


class PythonDependencyLinkerPlugin(DependencyLinkerPlugin):
    """Links Python files to dependency nodes via import analysis."""

    def __init__(self) -> None:
        self._import_map: dict[str, set[str]] | None = None

    def language(self) -> str:
        return "python"

    def extract_imports(self, source: str) -> set[str]:
        imports: set[str] = set()
        for match in _IMPORT_RE.finditer(source):
            imports.add(match.group(1).lower())
        for match in _FROM_IMPORT_RE.finditer(source):
            imports.add(match.group(1).lower())
        return imports

    def resolve_import_names(
        self,
        package_name: str,
        context: PostProcessContext,
    ) -> set[str]:
        # Lazily build the import map on first call
        if self._import_map is None:
            self._import_map = {}
            site_packages = _find_site_packages(context.root_path)
            if site_packages:
                self._import_map = _build_import_map(site_packages)
                logger.info(
                    "Python dependency linker: discovered import map for %d packages from %s",
                    len(self._import_map), site_packages,
                )

        # Normalize package name for lookup
        normalized = package_name.lower().replace("_", "-")

        # Try the discovered import map first
        if normalized in self._import_map:
            return self._import_map[normalized]

        # Fallback: standard PyPI normalization
        return {normalized.replace("-", "_")}
