"""Path collapsing for S3 object keys.

Variable segments (UUIDs, dates, numeric IDs, etc.) are replaced with
wildcard tokens so that millions of structurally similar keys collapse
into a small number of pattern strings.
"""

import re

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_HIVE_PARTITION_RE = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)=.+$")
_NUMERIC_ID_RE = re.compile(r"^\d{2,}$")
_ISO_TIMESTAMP_RE = re.compile(r"^\d{8}T\d{6}Z?$")
_HEX_HASH_RE = re.compile(r"^[0-9a-f]{16,}$", re.IGNORECASE)


def collapse_segment(segment: str) -> str:
    """Replace a variable path segment with a wildcard token.

    Returns the segment unchanged if it doesn't match any variable
    pattern.
    """
    if _UUID_RE.match(segment):
        return "{uuid}"
    if _ISO_DATE_RE.match(segment):
        return "{date}"
    m = _HIVE_PARTITION_RE.match(segment)
    if m:
        return f"{m.group(1)}={{*}}"
    if _NUMERIC_ID_RE.match(segment):
        return "{id}"
    if _ISO_TIMESTAMP_RE.match(segment):
        return "{timestamp}"
    if _HEX_HASH_RE.match(segment):
        return "{hash}"
    return segment


def collapse_key(key: str) -> str:
    """Collapse an entire S3 object key by wildcard-replacing each segment."""
    return "/".join(collapse_segment(s) for s in key.split("/"))


# ── Trie-based API (v2) ──────────────────────────────────────────

from src.graph.loaders.object_store.s3.matchers import (  # noqa: E402
    SegmentMatcher,
    SequenceMatcher,
    default_matchers,
)
from src.graph.loaders.object_store.s3.trie import PathTrie  # noqa: E402


def build_collapsed_trie(
    keys: list[str],
    *,
    sequence_matchers: list[SequenceMatcher] | None = None,
    segment_matchers: list[SegmentMatcher] | None = None,
    max_samples: int = 3,
) -> PathTrie:
    """Build a trie from S3 keys and collapse variable segments.

    Args:
        keys: Raw S3 object key strings.
        sequence_matchers: Multi-segment matchers (tried first).
        segment_matchers: Single-segment matchers.
        max_samples: Maximum sample keys to retain per node.

    Returns:
        A collapsed :class:`PathTrie` ready for graph node generation.
    """
    default_seq, default_seg = default_matchers()
    if sequence_matchers is None:
        sequence_matchers = default_seq
    if segment_matchers is None:
        segment_matchers = default_seg

    trie = PathTrie(max_samples=max_samples)
    for key in keys:
        trie.insert(key)
    trie.collapse(sequence_matchers, segment_matchers)
    return trie
