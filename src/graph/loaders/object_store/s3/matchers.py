"""Pluggable matchers for S3 path segment collapsing.

Matchers come in two flavors:

- **SegmentMatcher**: matches a single path segment (e.g. UUID, numeric ID).
- **SequenceMatcher**: matches consecutive segments (e.g. ``YYYY/MM/DD``).

Sequence matchers are tried first (greedy, longest match) so that
multi-segment patterns like time partitions are recognised before
individual segments would be collapsed as ``{id}``.
"""

from __future__ import annotations

import abc
import re


# ── Regex patterns (shared with wildcard.py) ──────────────────────

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_HIVE_PARTITION_RE = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)=.+$")
_NUMERIC_ID_RE = re.compile(r"^\d{2,}$")
_ISO_TIMESTAMP_RE = re.compile(r"^\d{8}T\d{6}Z?$")
_HEX_HASH_RE = re.compile(r"^[0-9a-f]{16,}$", re.IGNORECASE)

_YEAR_RE = re.compile(r"^20\d{2}$")
_MONTH_RE = re.compile(r"^(0[1-9]|1[0-2])$")
_DAY_RE = re.compile(r"^(0[1-9]|[12]\d|3[01])$")
_HOUR_RE = re.compile(r"^([01]\d|2[0-3])$")

_HIVE_YEAR_RE = re.compile(r"^year=(\d{4}|{\*})$")
_HIVE_MONTH_RE = re.compile(r"^month=(\d{1,2}|{\*})$")
_HIVE_DAY_RE = re.compile(r"^day=(\d{1,2}|{\*})$")
_HIVE_HOUR_RE = re.compile(r"^hour=(\d{1,2}|{\*})$")


# ── ABCs ──────────────────────────────────────────────────────────


class SegmentMatcher(abc.ABC):
    """Matches a single path segment and returns a wildcard token."""

    @property
    @abc.abstractmethod
    def token(self) -> str:
        """The wildcard token this matcher produces (e.g. ``{uuid}``)."""

    @abc.abstractmethod
    def matches(self, segment: str) -> bool:
        """Return True if *segment* matches this pattern."""

    def replacement(self, segment: str) -> str:
        """Return the wildcard string for *segment*.

        Defaults to :pyattr:`token`.  Override for matchers whose
        replacement depends on the input (e.g. hive partitions).
        """
        return self.token


class SequenceMatcher(abc.ABC):
    """Matches a contiguous run of path segments."""

    @property
    @abc.abstractmethod
    def token(self) -> str:
        """The wildcard token for the collapsed sequence."""

    @abc.abstractmethod
    def match_length(self, segments: list[str]) -> int:
        """Return how many leading segments are consumed, or 0 for no match."""


# ── Segment matchers ──────────────────────────────────────────────


class UUIDMatcher(SegmentMatcher):
    @property
    def token(self) -> str:
        return "{uuid}"

    def matches(self, segment: str) -> bool:
        return bool(_UUID_RE.match(segment))


class ISODateMatcher(SegmentMatcher):
    @property
    def token(self) -> str:
        return "{date}"

    def matches(self, segment: str) -> bool:
        return bool(_ISO_DATE_RE.match(segment))


class NumericIDMatcher(SegmentMatcher):
    @property
    def token(self) -> str:
        return "{id}"

    def matches(self, segment: str) -> bool:
        return bool(_NUMERIC_ID_RE.match(segment))


class ISOTimestampMatcher(SegmentMatcher):
    @property
    def token(self) -> str:
        return "{timestamp}"

    def matches(self, segment: str) -> bool:
        return bool(_ISO_TIMESTAMP_RE.match(segment))


class HexHashMatcher(SegmentMatcher):
    @property
    def token(self) -> str:
        return "{hash}"

    def matches(self, segment: str) -> bool:
        return bool(_HEX_HASH_RE.match(segment))


class HivePartitionMatcher(SegmentMatcher):
    """Matches ``key=value`` hive-style segments.

    The replacement is dynamic: ``year=2024`` → ``year={*}``.
    """

    @property
    def token(self) -> str:
        return "{hive_partition}"

    def matches(self, segment: str) -> bool:
        return bool(_HIVE_PARTITION_RE.match(segment))

    def replacement(self, segment: str) -> str:
        m = _HIVE_PARTITION_RE.match(segment)
        if m:
            return f"{m.group(1)}={{*}}"
        return segment


# ── Sequence matchers ─────────────────────────────────────────────


class TimePartitionMatcher(SequenceMatcher):
    """Detects bare ``YYYY/MM/DD[/HH]`` numeric time partitions.

    Also matches already-collapsed tokens: a 4-digit year followed by
    ``{id}`` segments (produced when month/day siblings are merged).
    """

    @property
    def token(self) -> str:
        return "{time_partition}"

    def match_length(self, segments: list[str]) -> int:
        if len(segments) < 3:
            return 0
        # Year must be raw 4-digit year
        if not _YEAR_RE.match(segments[0]):
            return 0
        # Month and day can be raw or already-collapsed {id}
        if not (segments[1] == "{id}" or _MONTH_RE.match(segments[1])):
            return 0
        if not (segments[2] == "{id}" or _DAY_RE.match(segments[2])):
            return 0
        if len(segments) >= 4 and (
            segments[3] == "{id}" or _HOUR_RE.match(segments[3])
        ):
            return 4
        return 3


class HiveTimePartitionMatcher(SequenceMatcher):
    """Detects ``year=*/month=*/day=*[/hour=*]`` hive time partitions.

    Works on both raw (``year=2024``) and already-collapsed
    (``year={*}``) segments.
    """

    @property
    def token(self) -> str:
        return "{hive_time_partition}"

    def match_length(self, segments: list[str]) -> int:
        if len(segments) < 3:
            return 0
        if not _HIVE_YEAR_RE.match(segments[0]):
            return 0
        if not _HIVE_MONTH_RE.match(segments[1]):
            return 0
        if not _HIVE_DAY_RE.match(segments[2]):
            return 0
        if len(segments) >= 4 and _HIVE_HOUR_RE.match(segments[3]):
            return 4
        return 3


# ── Factory ───────────────────────────────────────────────────────


def default_matchers() -> tuple[list[SequenceMatcher], list[SegmentMatcher]]:
    """Return ``(sequence_matchers, segment_matchers)`` with built-in defaults.

    Sequence matchers are in priority order (tried first-to-last).
    Segment matchers are in specificity order (most specific first).
    """
    return (
        [HiveTimePartitionMatcher(), TimePartitionMatcher()],
        [
            UUIDMatcher(),
            ISODateMatcher(),
            HivePartitionMatcher(),
            NumericIDMatcher(),
            ISOTimestampMatcher(),
            HexHashMatcher(),
        ],
    )
