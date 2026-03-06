"""Sensitive data classification for graph nodes.

Classifies column names and S3 paths against known sensitivity patterns
and tags matching nodes with DATA_SENSITIVITY metadata.
"""

from __future__ import annotations

import re

from src.graph.graph_models import Node, NodeMetadataKey

NK = NodeMetadataKey

# Pattern categories: {tag: compiled_regex}
# Each regex matches the full column name (case-insensitive).
_COLUMN_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("pii.email", re.compile(r".*(?:^|_)e?_?mail(?:_address)?(?:$|_).*", re.IGNORECASE)),
    ("pii.phone", re.compile(r".*(?:^|_)(?:phone|mobile|cell|telephone)(?:$|_).*", re.IGNORECASE)),
    ("pii.ssn", re.compile(r".*(?:^|_)(?:ssn|social_security|tax_id)(?:$|_).*", re.IGNORECASE)),
    ("pii.name", re.compile(r".*(?:^|_)(?:first_name|last_name|full_name|surname)(?:$|_).*", re.IGNORECASE)),
    ("pii.address", re.compile(r".*(?:^|_)(?:address|street|city|zip_code|postal)(?:$|_).*", re.IGNORECASE)),
    ("secret.password", re.compile(r".*(?:^|_)(?:password|passwd|pwd|pass_hash)(?:$|_).*", re.IGNORECASE)),
    ("secret.token", re.compile(r".*(?:^|_)(?:token|api_key|secret_key|access_key|refresh_token)(?:$|_).*", re.IGNORECASE)),
    ("financial.amount", re.compile(r".*(?:^|_)(?:balance|amount|price|salary)(?:$|_).*", re.IGNORECASE)),
    ("financial.account", re.compile(r".*(?:^|_)(?:account_number|routing_number|iban|card_number)(?:$|_).*", re.IGNORECASE)),
]

_S3_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("pii", re.compile(r".*\bpii\b.*", re.IGNORECASE)),
    ("secret", re.compile(r".*\bsecrets?\b.*", re.IGNORECASE)),
    ("financial", re.compile(r".*\bfinancial\b.*", re.IGNORECASE)),
    ("pii.address", re.compile(r".*\baddress(?:es)?\b.*", re.IGNORECASE)),
]


def classify_column_name(name: str) -> list[str]:
    """Return sensitivity tags matching the given column name."""
    tags = []
    for tag, pattern in _COLUMN_PATTERNS:
        if pattern.match(name):
            tags.append(tag)
    return tags


def classify_s3_path(path: str) -> list[str]:
    """Return sensitivity tags matching the given S3 path."""
    tags = []
    for tag, pattern in _S3_PATTERNS:
        if pattern.match(path):
            tags.append(tag)
    return tags


def enrich_sensitivity(nodes: list[Node]) -> list[Node]:
    """Walk all nodes and tag sensitive columns/S3 paths with DATA_SENSITIVITY.

    Also propagates sensitivity to parent table nodes.
    """
    # Build parent URN -> node mapping for propagation
    parent_sensitivity: dict[str, set[str]] = {}

    for node in nodes:
        tags: list[str] = []

        # Classify columns
        col_name = node.metadata.get(NK.COLUMN_NAME)
        if col_name:
            tags = classify_column_name(col_name)

        # Classify S3 paths
        path_pattern = node.metadata.get(NK.PATH_PATTERN)
        if path_pattern:
            tags = classify_s3_path(path_pattern)

        if tags:
            existing = node.metadata.get(NK.DATA_SENSITIVITY)
            if existing:
                all_tags = set(existing.split(",")) | set(tags)
                tags = sorted(all_tags)
            node.metadata[NK.DATA_SENSITIVITY] = ",".join(tags)

            # Track parent sensitivity for propagation
            if node.parent_urn:
                parent_key = str(node.parent_urn)
                parent_sensitivity.setdefault(parent_key, set()).update(tags)

    # Propagate to parent tables
    for node in nodes:
        urn_str = str(node.urn)
        if urn_str in parent_sensitivity and NK.TABLE_NAME in node.metadata:
            inherited = parent_sensitivity[urn_str]
            existing = node.metadata.get(NK.DATA_SENSITIVITY)
            if existing:
                inherited = inherited | set(existing.split(","))
            node.metadata[NK.DATA_SENSITIVITY] = ",".join(sorted(inherited))

    return nodes
