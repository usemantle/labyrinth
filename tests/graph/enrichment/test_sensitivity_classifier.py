"""Tests for the sensitivity classifier enrichment module."""

import uuid

import pytest

from src.graph.graph_models import Node, NodeMetadata, NodeMetadataKey, URN
from src.graph.enrichment.sensitivity_classifier import (
    classify_column_name,
    classify_s3_path,
    enrich_sensitivity,
)

NK = NodeMetadataKey
ORG_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


def _make_column_node(column_name: str, table_urn: str = "urn:test:db:::mydb/public/users") -> Node:
    return Node(
        organization_id=ORG_ID,
        urn=URN(f"{table_urn}/{column_name}"),
        parent_urn=URN(table_urn),
        metadata=NodeMetadata({
            NK.COLUMN_NAME: column_name,
            NK.DATA_TYPE: "text",
        }),
    )


def _make_table_node(table_name: str, urn: str = "urn:test:db:::mydb/public/users") -> Node:
    return Node(
        organization_id=ORG_ID,
        urn=URN(urn),
        parent_urn=URN("urn:test:db:::mydb/public"),
        metadata=NodeMetadata({
            NK.TABLE_NAME: table_name,
            NK.TABLE_TYPE: "BASE_TABLE",
        }),
    )


def _make_s3_node(path: str) -> Node:
    return Node(
        organization_id=ORG_ID,
        urn=URN(f"urn:aws:s3:::my-bucket/{path}"),
        parent_urn=URN("urn:aws:s3:::my-bucket"),
        metadata=NodeMetadata({
            NK.PATH_PATTERN: path,
        }),
    )


# ── classify_column_name tests ────────────────────────────────────────


class TestClassifyColumnName:
    def test_email_column_classified(self):
        tags = classify_column_name("email")
        assert "pii.email" in tags

    def test_email_address_classified(self):
        tags = classify_column_name("email_address")
        assert "pii.email" in tags

    def test_password_column_classified(self):
        tags = classify_column_name("password_hash")
        assert "secret.password" in tags

    def test_non_sensitive_column_ignored(self):
        tags = classify_column_name("created_at")
        assert tags == []

    def test_multiple_sensitivities(self):
        # A column named "email_token" should match both email and token
        tags = classify_column_name("email_token")
        assert "pii.email" in tags
        assert "secret.token" in tags

    def test_case_insensitive_matching(self):
        tags = classify_column_name("EMAIL")
        assert "pii.email" in tags

    def test_phone_column(self):
        tags = classify_column_name("phone_number")
        assert "pii.phone" in tags

    def test_ssn_column(self):
        tags = classify_column_name("ssn")
        assert "pii.ssn" in tags

    def test_column_name_variations(self):
        assert "pii.name" in classify_column_name("first_name")
        assert "pii.name" in classify_column_name("last_name")
        assert "pii.name" in classify_column_name("full_name")

    def test_financial_columns(self):
        assert "financial.amount" in classify_column_name("salary")
        assert "financial.account" in classify_column_name("account_number")


# ── classify_s3_path tests ────────────────────────────────────────────


class TestClassifyS3Path:
    def test_s3_path_pii(self):
        tags = classify_s3_path("data/pii/exports/")
        assert "pii" in tags

    def test_s3_path_secrets(self):
        tags = classify_s3_path("config/secrets/keys/")
        assert "secret" in tags

    def test_s3_path_no_match(self):
        tags = classify_s3_path("logs/application/2024/")
        assert tags == []


# ── enrich_sensitivity tests ──────────────────────────────────────────


class TestEnrichSensitivity:
    def test_table_inherits_sensitivity(self):
        table = _make_table_node("users")
        col = _make_column_node("email")
        nodes = enrich_sensitivity([table, col])

        # Column should be tagged
        assert nodes[1].metadata.get(NK.DATA_SENSITIVITY) is not None
        assert "pii.email" in nodes[1].metadata[NK.DATA_SENSITIVITY]

        # Table should inherit
        assert nodes[0].metadata.get(NK.DATA_SENSITIVITY) is not None
        assert "pii.email" in nodes[0].metadata[NK.DATA_SENSITIVITY]

    def test_enrichment_preserves_existing_metadata(self):
        col = _make_column_node("email")
        col.metadata[NK.DATA_SENSITIVITY] = "custom.tag"
        nodes = enrich_sensitivity([col])
        sensitivity = nodes[0].metadata[NK.DATA_SENSITIVITY]
        assert "custom.tag" in sensitivity
        assert "pii.email" in sensitivity

    def test_enrichment_with_no_columns(self):
        # Code-only graph should not crash
        func_node = Node(
            organization_id=ORG_ID,
            urn=URN("urn:github:repo:::myapp/src/main.py/my_func"),
            parent_urn=URN("urn:github:repo:::myapp/src/main.py"),
            metadata=NodeMetadata({
                NK.FUNCTION_NAME: "my_func",
                NK.FILE_PATH: "src/main.py",
            }),
        )
        nodes = enrich_sensitivity([func_node])
        assert len(nodes) == 1
        assert NK.DATA_SENSITIVITY not in nodes[0].metadata

    def test_s3_node_enrichment(self):
        s3_node = _make_s3_node("data/pii/exports/")
        nodes = enrich_sensitivity([s3_node])
        assert "pii" in nodes[0].metadata[NK.DATA_SENSITIVITY]
