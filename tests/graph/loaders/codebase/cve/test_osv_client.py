"""Unit tests for the OSV.dev CVE data sourcer."""

import io
import json
from unittest.mock import patch, MagicMock
from urllib.error import URLError, HTTPError

from src.graph.loaders.codebase.cve.osv_client import query_osv


def _mock_response(data: dict) -> MagicMock:
    """Create a mock urllib response with JSON data."""
    body = json.dumps(data).encode("utf-8")
    resp = MagicMock()
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


@patch("src.graph.loaders.codebase.cve.osv_client.urllib.request.urlopen")
def test_vulnerable_package(mock_urlopen):
    mock_urlopen.return_value = _mock_response({
        "vulns": [
            {"id": "CVE-2021-12345", "aliases": ["GHSA-abcd-efgh-ijkl"]},
        ]
    })
    result = query_osv("requests", "2.25.0", "PyPI")
    assert result.cve_ids == ["CVE-2021-12345"]
    assert result.ghsa_ids == ["GHSA-abcd-efgh-ijkl"]
    assert result.error is None


@patch("src.graph.loaders.codebase.cve.osv_client.urllib.request.urlopen")
def test_clean_package(mock_urlopen):
    mock_urlopen.return_value = _mock_response({"vulns": []})
    result = query_osv("requests", "2.31.0", "PyPI")
    assert result.cve_ids == []
    assert result.ghsa_ids == []
    assert result.error is None


@patch("src.graph.loaders.codebase.cve.osv_client.urllib.request.urlopen")
def test_missing_vulns_key(mock_urlopen):
    mock_urlopen.return_value = _mock_response({})
    result = query_osv("unknown-pkg", "1.0.0", "PyPI")
    assert result.cve_ids == []
    assert result.ghsa_ids == []
    assert result.error is None


@patch("src.graph.loaders.codebase.cve.osv_client.urllib.request.urlopen")
def test_network_error(mock_urlopen):
    mock_urlopen.side_effect = URLError("Connection refused")
    result = query_osv("requests", "2.25.0", "PyPI")
    assert result.cve_ids == []
    assert result.error is not None
    assert "Connection refused" in result.error


@patch("src.graph.loaders.codebase.cve.osv_client.urllib.request.urlopen")
def test_http_500_error(mock_urlopen):
    mock_urlopen.side_effect = HTTPError(
        url="https://api.osv.dev/v1/query",
        code=500,
        msg="Internal Server Error",
        hdrs=None,
        fp=io.BytesIO(b""),
    )
    result = query_osv("requests", "2.25.0", "PyPI")
    assert result.cve_ids == []
    assert result.error is not None


@patch("src.graph.loaders.codebase.cve.osv_client.urllib.request.urlopen")
def test_correct_payload(mock_urlopen):
    mock_urlopen.return_value = _mock_response({"vulns": []})
    query_osv("flask", "2.0.0", "PyPI")

    call_args = mock_urlopen.call_args
    req = call_args[0][0]
    payload = json.loads(req.data.decode("utf-8"))
    assert payload == {
        "version": "2.0.0",
        "package": {"name": "flask", "ecosystem": "PyPI"},
    }


@patch("src.graph.loaders.codebase.cve.osv_client.urllib.request.urlopen")
def test_dedup_aliases(mock_urlopen):
    mock_urlopen.return_value = _mock_response({
        "vulns": [
            {
                "id": "GHSA-xxxx-yyyy-zzzz",
                "aliases": ["CVE-2021-99999"],
            },
            {
                "id": "CVE-2021-99999",
                "aliases": ["GHSA-xxxx-yyyy-zzzz"],
            },
        ]
    })
    result = query_osv("pkg", "1.0.0", "PyPI")
    assert result.cve_ids == ["CVE-2021-99999"]
    assert result.ghsa_ids == ["GHSA-xxxx-yyyy-zzzz"]
