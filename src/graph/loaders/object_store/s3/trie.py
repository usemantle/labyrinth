"""Trie-based path hierarchy for S3 object keys.

Builds a prefix trie from raw S3 keys, then applies matchers to
collapse variable segments into wildcard tokens while preserving the
tree structure.

Usage::

    trie = PathTrie()
    for key in keys:
        trie.insert(key)
    trie.collapse(sequence_matchers, segment_matchers)
    for path_segments, node in trie.walk():
        ...
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Iterator

from src.graph.loaders.object_store.s3.matchers import (
    SegmentMatcher,
    SequenceMatcher,
)

logger = logging.getLogger(__name__)

_MAX_SAMPLES = 3


@dataclasses.dataclass
class TrieNode:
    """A single node in the path trie."""

    segment: str
    children: dict[str, TrieNode] = dataclasses.field(default_factory=dict)
    key_count: int = 0
    is_leaf: bool = False
    sample_keys: list[str] = dataclasses.field(default_factory=list)
    collapsed_token: str | None = None


class PathTrie:
    """Prefix trie that collapses variable S3 path segments."""

    def __init__(self, *, max_samples: int = _MAX_SAMPLES) -> None:
        self.root = TrieNode(segment="")
        self._max_samples = max_samples

    # ── Insertion ─────────────────────────────────────────────────

    def insert(self, key: str) -> None:
        """Insert an S3 object key into the trie."""
        parts = key.split("/")
        node = self.root
        for part in parts:
            if part not in node.children:
                node.children[part] = TrieNode(segment=part)
            node = node.children[part]
            node.key_count += 1
            if len(node.sample_keys) < self._max_samples:
                node.sample_keys.append(key)
        node.is_leaf = True

    # ── Collapsing ────────────────────────────────────────────────

    def collapse(
        self,
        sequence_matchers: list[SequenceMatcher],
        segment_matchers: list[SegmentMatcher],
    ) -> None:
        """Collapse variable segments bottom-up."""
        self._collapse_node(self.root, sequence_matchers, segment_matchers)

    def _collapse_node(
        self,
        node: TrieNode,
        sequence_matchers: list[SequenceMatcher],
        segment_matchers: list[SegmentMatcher],
    ) -> None:
        # 1. Recurse into children first (bottom-up)
        for child in list(node.children.values()):
            self._collapse_node(child, sequence_matchers, segment_matchers)

        # 2. Sequence collapsing (before segment to preserve multi-segment patterns)
        self._collapse_sequences(node, sequence_matchers)

        # 3. Segment collapsing (merge siblings that all match the same matcher)
        self._collapse_segments(node, sequence_matchers, segment_matchers)

    def _collapse_sequences(
        self,
        node: TrieNode,
        sequence_matchers: list[SequenceMatcher],
    ) -> None:
        """Squash single-child chains that match a sequence pattern."""
        for child_key in list(node.children.keys()):
            if child_key not in node.children:
                continue
            child = node.children[child_key]

            # Build the single-child chain starting from this child
            chain = [child]
            cursor = child
            while len(cursor.children) == 1:
                cursor = next(iter(cursor.children.values()))
                chain.append(cursor)

            if len(chain) < 2:
                continue

            chain_segments = [n.segment for n in chain]
            for matcher in sequence_matchers:
                consumed = matcher.match_length(chain_segments)
                if consumed >= 2:
                    self._squash_chain(
                        node, child_key, chain, matcher.token, consumed,
                    )
                    break

    @staticmethod
    def _squash_chain(
        parent: TrieNode,
        child_key: str,
        chain: list[TrieNode],
        token: str,
        consumed: int,
    ) -> None:
        """Replace *consumed* chain levels with a single token node."""
        squashed = TrieNode(
            segment=token,
            children=chain[consumed - 1].children,
            collapsed_token=token,
            key_count=chain[0].key_count,
            sample_keys=chain[0].sample_keys[:_MAX_SAMPLES],
            is_leaf=chain[consumed - 1].is_leaf and not chain[consumed - 1].children,
        )
        del parent.children[child_key]
        parent.children[token] = squashed

    def _collapse_segments(
        self,
        node: TrieNode,
        sequence_matchers: list[SequenceMatcher],
        segment_matchers: list[SegmentMatcher],
    ) -> None:
        """Merge all children of *node* when they all match one matcher.

        After merging, the merged node is re-collapsed because new sibling
        relationships may have formed (e.g. merging months surfaces days
        that can then be merged, enabling sequence detection).
        """
        if len(node.children) <= 1:
            return

        for matcher in segment_matchers:
            children_list = list(node.children.values())
            if not all(matcher.matches(c.segment) for c in children_list):
                continue

            # All children match — verify they produce the same token
            tokens = {matcher.replacement(c.segment) for c in children_list}
            if len(tokens) != 1:
                continue

            token = tokens.pop()
            merged = self._merge_siblings(children_list, token)
            node.children = {token: merged}
            # Re-collapse: merging may have created new sibling groups
            self._collapse_node(merged, sequence_matchers, segment_matchers)
            return

    def _merge_siblings(
        self, siblings: list[TrieNode], token: str,
    ) -> TrieNode:
        """Create a single wildcard node by merging sibling subtrees."""
        merged = TrieNode(segment=token, collapsed_token=token)
        for sibling in siblings:
            merged.key_count += sibling.key_count
            for s in sibling.sample_keys:
                if len(merged.sample_keys) < self._max_samples:
                    merged.sample_keys.append(s)
            merged.is_leaf = merged.is_leaf or sibling.is_leaf
            self._merge_children(merged, sibling)
        return merged

    def _merge_children(self, target: TrieNode, source: TrieNode) -> None:
        """Recursively merge *source*'s children into *target*."""
        for seg, child in source.children.items():
            if seg in target.children:
                existing = target.children[seg]
                existing.key_count += child.key_count
                for s in child.sample_keys:
                    if len(existing.sample_keys) < self._max_samples:
                        existing.sample_keys.append(s)
                existing.is_leaf = existing.is_leaf or child.is_leaf
                self._merge_children(existing, child)
            else:
                target.children[seg] = child

    # ── Walking ───────────────────────────────────────────────────

    def walk(self) -> Iterator[tuple[list[str], TrieNode]]:
        """Yield ``(path_segments, node)`` for every node in the trie.

        Traversal is depth-first with sorted children for determinism.
        """
        stack: list[tuple[list[str], TrieNode]] = []
        for seg in sorted(self.root.children, reverse=True):
            stack.append(([seg], self.root.children[seg]))

        while stack:
            path, node = stack.pop()
            yield path, node
            for seg in sorted(node.children, reverse=True):
                stack.append((path + [seg], node.children[seg]))
