"""Tests for S3 path trie, matchers, and hierarchical collapsing."""

import pytest

from src.graph.loaders.object_store.s3.matchers import (
    HexHashMatcher,
    HivePartitionMatcher,
    HiveTimePartitionMatcher,
    ISODateMatcher,
    ISOTimestampMatcher,
    NumericIDMatcher,
    TimePartitionMatcher,
    UUIDMatcher,
    default_matchers,
)
from src.graph.loaders.object_store.s3.trie import PathTrie
from src.graph.loaders.object_store.s3.wildcard import build_collapsed_trie

# ── Segment matcher tests ─────────────────────────────────────────


class TestUUIDMatcher:
    m = UUIDMatcher()

    def test_matches(self):
        assert self.m.matches("550e8400-e29b-41d4-a716-446655440000")

    def test_uppercase(self):
        assert self.m.matches("550E8400-E29B-41D4-A716-446655440000")

    def test_rejects(self):
        assert not self.m.matches("not-a-uuid")

    def test_token(self):
        assert self.m.token == "{uuid}"


class TestISODateMatcher:
    m = ISODateMatcher()

    def test_matches(self):
        assert self.m.matches("2024-01-15")

    def test_rejects(self):
        assert not self.m.matches("20240115")


class TestNumericIDMatcher:
    m = NumericIDMatcher()

    def test_matches(self):
        assert self.m.matches("12345")

    def test_rejects_single_digit(self):
        assert not self.m.matches("7")


class TestISOTimestampMatcher:
    m = ISOTimestampMatcher()

    def test_matches_with_z(self):
        assert self.m.matches("20240115T103000Z")

    def test_matches_without_z(self):
        assert self.m.matches("20240115T103000")


class TestHexHashMatcher:
    m = HexHashMatcher()

    def test_matches(self):
        assert self.m.matches("a3f2b8c901d4e5f6a7b8")

    def test_rejects_short(self):
        assert not self.m.matches("a3f2b8c901d4e5f")


class TestHivePartitionMatcher:
    m = HivePartitionMatcher()

    def test_matches(self):
        assert self.m.matches("year=2024")

    def test_replacement(self):
        assert self.m.replacement("year=2024") == "year={*}"

    def test_underscore_key(self):
        assert self.m.replacement("event_type=click") == "event_type={*}"


# ── Sequence matcher tests ────────────────────────────────────────


class TestTimePartitionMatcher:
    m = TimePartitionMatcher()

    def test_ymd(self):
        assert self.m.match_length(["2024", "01", "15"]) == 3

    def test_ymdh(self):
        assert self.m.match_length(["2024", "01", "15", "08"]) == 4

    def test_rejects_short(self):
        assert self.m.match_length(["2024", "01"]) == 0

    def test_rejects_bad_month(self):
        assert self.m.match_length(["2024", "13", "01"]) == 0

    def test_rejects_bad_year(self):
        assert self.m.match_length(["1999", "01", "01"]) == 0

    def test_stops_at_non_hour(self):
        assert self.m.match_length(["2024", "01", "15", "file.txt"]) == 3


class TestHiveTimePartitionMatcher:
    m = HiveTimePartitionMatcher()

    def test_raw_ymd(self):
        assert self.m.match_length(["year=2024", "month=01", "day=15"]) == 3

    def test_raw_ymdh(self):
        assert self.m.match_length(
            ["year=2024", "month=01", "day=15", "hour=08"]
        ) == 4

    def test_collapsed_ymdh(self):
        assert self.m.match_length(
            ["year={*}", "month={*}", "day={*}", "hour={*}"]
        ) == 4

    def test_rejects_missing_month(self):
        assert self.m.match_length(["year=2024", "day=15"]) == 0

    def test_stops_at_non_hour(self):
        assert self.m.match_length(
            ["year=2024", "month=01", "day=15", "file.txt"]
        ) == 3


# ── Trie insertion tests ──────────────────────────────────────────


class TestTrieInsert:
    def test_single_key(self):
        trie = PathTrie()
        trie.insert("a/b/c.txt")
        assert "a" in trie.root.children
        assert "b" in trie.root.children["a"].children
        assert "c.txt" in trie.root.children["a"].children["b"].children

    def test_leaf_flag(self):
        trie = PathTrie()
        trie.insert("a/b.txt")
        assert not trie.root.children["a"].is_leaf
        assert trie.root.children["a"].children["b.txt"].is_leaf

    def test_key_count(self):
        trie = PathTrie()
        trie.insert("a/b.txt")
        trie.insert("a/c.txt")
        assert trie.root.children["a"].key_count == 2

    def test_branching(self):
        trie = PathTrie()
        trie.insert("a/b.txt")
        trie.insert("a/c.txt")
        assert len(trie.root.children["a"].children) == 2


# ── Trie segment collapsing tests ─────────────────────────────────


class TestTrieCollapseSegments:
    def test_uuid_siblings_merge(self):
        trie = build_collapsed_trie([
            "uploads/550e8400-e29b-41d4-a716-446655440000/photo.jpg",
            "uploads/661f9511-f30c-52e5-b827-557766551111/photo.jpg",
        ])
        # uploads -> {uuid} -> photo.jpg
        uploads = trie.root.children["uploads"]
        assert "{uuid}" in uploads.children
        uuid_node = uploads.children["{uuid}"]
        assert uuid_node.collapsed_token == "{uuid}"
        assert "photo.jpg" in uuid_node.children

    def test_date_siblings_merge(self):
        trie = build_collapsed_trie([
            "logs/2024-01-15/events.json",
            "logs/2024-02-20/events.json",
        ])
        logs = trie.root.children["logs"]
        assert "{date}" in logs.children
        assert logs.children["{date}"].collapsed_token == "{date}"

    def test_mixed_types_not_merged(self):
        """Siblings of different types should not be merged."""
        trie = build_collapsed_trie([
            "data/2024-01-15/report.csv",
            "data/config.json",
        ])
        data = trie.root.children["data"]
        # Should have 2 children: date + config.json
        assert len(data.children) == 2


# ── Trie sequence collapsing tests ────────────────────────────────


class TestTrieCollapseSequences:
    def test_bare_time_partition(self):
        trie = build_collapsed_trie([
            "data/2024/01/15/file.csv",
            "data/2024/02/20/file.csv",
        ])
        data = trie.root.children["data"]
        assert "{time_partition}" in data.children
        tp = data.children["{time_partition}"]
        assert tp.collapsed_token == "{time_partition}"
        assert "file.csv" in tp.children

    def test_hive_time_partition(self):
        trie = build_collapsed_trie([
            "logs/year=2025/month=12/day=29/hour=16/data.gz",
            "logs/year=2026/month=01/day=03/hour=02/data.gz",
        ])
        logs = trie.root.children["logs"]
        assert "{hive_time_partition}" in logs.children
        htp = logs.children["{hive_time_partition}"]
        assert htp.collapsed_token == "{hive_time_partition}"
        assert "data.gz" in htp.children

    def test_hive_time_partition_without_hour(self):
        trie = build_collapsed_trie([
            "logs/year=2025/month=12/day=29/data.gz",
            "logs/year=2026/month=01/day=03/data.gz",
        ])
        logs = trie.root.children["logs"]
        assert "{hive_time_partition}" in logs.children


# ── Mixed path tests ──────────────────────────────────────────────


class TestTrieMixedPaths:
    def test_uuid_then_time_partition(self):
        trie = build_collapsed_trie([
            "data/550e8400-e29b-41d4-a716-446655440000/2024/01/15/file.csv",
            "data/661f9511-f30c-52e5-b827-557766551111/2024/02/20/file.csv",
        ])
        data = trie.root.children["data"]
        uuid_node = data.children["{uuid}"]
        assert "{time_partition}" in uuid_node.children
        tp = uuid_node.children["{time_partition}"]
        assert "file.csv" in tp.children


# ── Static prefix tests ───────────────────────────────────────────


class TestTrieStaticPrefixes:
    def test_shared_prefix_becomes_node(self):
        trie = build_collapsed_trie([
            "static/images/logo.png",
            "static/images/banner.png",
            "static/css/main.css",
        ])
        assert "static" in trie.root.children
        static = trie.root.children["static"]
        assert "images" in static.children
        assert "css" in static.children

    def test_prefix_not_leaf(self):
        trie = build_collapsed_trie(["a/b/c.txt"])
        assert not trie.root.children["a"].is_leaf
        assert not trie.root.children["a"].children["b"].is_leaf
        assert trie.root.children["a"].children["b"].children["c.txt"].is_leaf


# ── Walk tests ────────────────────────────────────────────────────


class TestTrieWalk:
    def test_walk_order_and_content(self):
        trie = build_collapsed_trie(["a/b.txt", "a/c.txt"])
        paths = ["/".join(segs) for segs, _ in trie.walk()]
        assert "a" in paths
        assert "a/b.txt" in paths
        assert "a/c.txt" in paths

    def test_walk_deterministic(self):
        trie1 = build_collapsed_trie(["x/y.txt", "x/z.txt"])
        trie2 = build_collapsed_trie(["x/z.txt", "x/y.txt"])
        paths1 = ["/".join(s) for s, _ in trie1.walk()]
        paths2 = ["/".join(s) for s, _ in trie2.walk()]
        assert paths1 == paths2


# ── Real-world pattern test ───────────────────────────────────────


class TestTrieRealWorldAuditLogs:
    """Reproduces the actual dsec-log-export-test bucket patterns."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.keys = [
            "audit_logs/dogfood/year=2025/month=12/day=29/hour=16/logs_20251229_160402.jsonl.gz",
            "audit_logs/dogfood2/year=2026/month=01/day=02/hour=20/logs_20260102_205403.jsonl.gz",
            "audit_logs/dogfood2/year=2026/month=01/day=03/hour=01/logs_20260103_015502.jsonl.gz",
            "audit_logs/dogfood2/year=2026/month=01/day=03/hour=02/logs_20260103_023210.jsonl.gz",
            "audit_logs/dogfood2/year=2026/month=01/day=03/hour=02/logs_20260103_023510.jsonl.gz",
            "audit_logs/dogfood2/year=2026/month=01/day=03/hour=15/logs_20260103_155501.jsonl.gz",
            "audit_logs/dogfood2/year=2026/month=01/day=03/hour=17/logs_20260103_172015.jsonl.gz",
            "audit_logs/dogfood2/year=2026/month=01/day=05/hour=00/logs_20260105_003911.jsonl.gz",
            "audit_logs/newone/year=2026/month=01/day=05/hour=02/logs_20260105_020537.jsonl.gz",
        ]
        self.trie = build_collapsed_trie(self.keys)

    def test_top_level_prefix(self):
        assert "audit_logs" in self.trie.root.children
        assert len(self.trie.root.children) == 1

    def test_org_prefixes(self):
        al = self.trie.root.children["audit_logs"]
        assert "dogfood" in al.children
        assert "dogfood2" in al.children
        assert "newone" in al.children

    def test_hive_time_partition_collapsed(self):
        dogfood2 = self.trie.root.children["audit_logs"].children["dogfood2"]
        assert "{hive_time_partition}" in dogfood2.children

    def test_leaf_files_under_partition(self):
        htp = (
            self.trie.root.children["audit_logs"]
            .children["dogfood2"]
            .children["{hive_time_partition}"]
        )
        # All leaf filenames are different and don't match simple segment matchers
        assert len(htp.children) > 0


# ── default_matchers tests ────────────────────────────────────────


class TestDefaultMatchers:
    def test_returns_two_lists(self):
        seq, seg = default_matchers()
        assert len(seq) >= 2
        assert len(seg) >= 6
