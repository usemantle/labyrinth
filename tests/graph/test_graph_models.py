"""Unit tests for security and dependency metadata keys in graph_models."""

import pytest

from src.graph.graph_models import NodeMetadata, NodeMetadataKey, edge_namespace

NK = NodeMetadataKey


class TestEdgeNamespace:
    """edge_namespace() extracts the namespace prefix from an edge type string."""

    @pytest.mark.parametrize("edge_type,expected", [
        ("idp:assigned_to", "idp"),
        ("idp:maps_to", "idp"),
        ("idp:part_of", "idp"),
        ("idp:pushes_to", "idp"),
        ("foo:bar:baz", "foo"),  # only the first colon is the separator
    ])
    def test_prefixed(self, edge_type, expected):
        assert edge_namespace(edge_type) == expected

    @pytest.mark.parametrize("edge_type", [
        "contains",
        "reads",
        "writes",
        "member_of",
    ])
    def test_unprefixed_returns_none(self, edge_type):
        assert edge_namespace(edge_type) is None

    def test_empty_returns_none(self):
        assert edge_namespace("") is None


class TestSecurityMetadataKeys:
    """Verify each new security/dependency key exists and is usable."""

    def test_io_direction_exists(self):
        assert NK.IO_DIRECTION.value == "io_direction"

    def test_io_type_exists(self):
        assert NK.IO_TYPE.value == "io_type"

    def test_cve_ids_exists(self):
        assert NK.CVE_IDS.value == "cve_ids"

    def test_data_sensitivity_exists(self):
        assert NK.DATA_SENSITIVITY.value == "data_sensitivity"

    def test_package_name_exists(self):
        assert NK.PACKAGE_NAME.value == "package_name"

    def test_package_version_exists(self):
        assert NK.PACKAGE_VERSION.value == "package_version"

    def test_package_ecosystem_exists(self):
        assert NK.PACKAGE_ECOSYSTEM.value == "package_ecosystem"

    @pytest.mark.parametrize("key", [
        NK.IO_DIRECTION,
        NK.IO_TYPE,
        NK.CVE_IDS,
        NK.DATA_SENSITIVITY,
        NK.PACKAGE_NAME,
        NK.PACKAGE_VERSION,
        NK.PACKAGE_ECOSYSTEM,
    ])
    def test_settable_on_node_metadata(self, key):
        meta = NodeMetadata()
        meta[key] = "test_value"
        assert meta[key] == "test_value"
