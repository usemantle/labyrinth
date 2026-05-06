"""OSV.dev API client for querying package vulnerabilities.

Uses only stdlib (urllib.request) — no new dependencies required.
"""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass, field

OSV_QUERY_URL = "https://api.osv.dev/v1/query"


@dataclass
class OsvResult:
    """Result of a vulnerability query against OSV.dev."""

    cve_ids: list[str] = field(default_factory=list)
    ghsa_ids: list[str] = field(default_factory=list)
    error: str | None = None


def query_osv(name: str, version: str, ecosystem: str) -> OsvResult:
    """Query OSV.dev for vulnerabilities in a specific package version.

    Args:
        name: Package name (e.g. "requests").
        version: Package version (e.g. "2.25.0").
        ecosystem: Package ecosystem (e.g. "PyPI").

    Returns:
        OsvResult with deduplicated CVE/GHSA IDs, or an error string
        if the request failed. Never raises exceptions.
    """
    payload = json.dumps({
        "version": version,
        "package": {"name": name, "ecosystem": ecosystem},
    }).encode("utf-8")

    req = urllib.request.Request(
        OSV_QUERY_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        return OsvResult(error=str(exc))

    vulns = data.get("vulns", [])
    cve_ids: set[str] = set()
    ghsa_ids: set[str] = set()

    for vuln in vulns:
        # Primary ID
        vuln_id = vuln.get("id", "")
        if vuln_id.startswith("CVE-"):
            cve_ids.add(vuln_id)
        elif vuln_id.startswith("GHSA-"):
            ghsa_ids.add(vuln_id)

        # Aliases
        for alias in vuln.get("aliases", []):
            if alias.startswith("CVE-"):
                cve_ids.add(alias)
            elif alias.startswith("GHSA-"):
                ghsa_ids.add(alias)

    return OsvResult(
        cve_ids=sorted(cve_ids),
        ghsa_ids=sorted(ghsa_ids),
    )
