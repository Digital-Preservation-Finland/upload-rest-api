"""Unit tests for metadata generation."""
import os
import pathlib

import pytest

import upload_rest_api.gen_metadata as md
import upload_rest_api.database as db


@pytest.mark.parametrize(
    "fpath,upload_path,expected",
    [
        ("/upload/project/fpath", "/upload", "/fpath"),
        ("/test/project/fpath", "/test", "/fpath"),
        ("/upload///project///fpath", "/upload", "/fpath")
    ]
)
def test_metax_path(fpath, upload_path, expected):
    """Test fpath is sliced properly and returns path
    /project/<path:fpath>.
    """
    assert md.get_metax_path(pathlib.Path(fpath), upload_path) == expected


def test_mimetype():
    """Test that _get_mimetype() returns correct MIME types."""
    assert md._get_mimetype("tests/data/test.txt") == "text/plain"
    assert md._get_mimetype("tests/data/test.zip") == "application/zip"


def test_gen_metadata(monkeypatch, mock_config):
    """Test that _generate_metadata() produces the correct metadata."""
    fpath = os.path.abspath("tests/data/test.txt")

    monkeypatch.setattr(
        db.Files, "get_path_checksum_dict",
        lambda self: {
            fpath: "150b62e4e7d58c70503bd5fc8a26463c"
        }
    )
    metadata = md._generate_metadata(
        "tests/data/test.txt",
        "tests", "data",
        db.Database().files.get_path_checksum_dict()
    )

    assert len(metadata["identifier"]) == 45
    assert metadata["file_name"] == "test.txt"
    assert metadata["file_format"] == "text/plain"
    assert metadata["byte_size"] == 31
    assert metadata["file_path"] == "/test.txt"
    assert metadata["project_identifier"] == "data"
    assert "file_uploaded" in metadata
    assert "file_modified" in metadata
    assert "file_frozen" in metadata

    checksum = metadata["checksum"]
    assert checksum["algorithm"] == "MD5"
    assert checksum["value"] == "150b62e4e7d58c70503bd5fc8a26463c"
    assert "checked" in checksum

    assert metadata["file_storage"] == mock_config["STORAGE_ID"]


@pytest.mark.parametrize('verify', [True, False])
def test_metax_ssl_verification(requests_mock, verify):
    """Test Metax HTTPS connection verification.

    HTTPS connection to Metax should be verified if `verify` parameter
    is used.

    :param requests_mock: HTTP request mocker
    :param verify: value for MetaxClient `verify` parameter
    """
    requests_mock.get('https://foo/rest/v2/datasets/qux', json={})

    md.MetaxClient(url='https://foo',
                   user='bar',
                   password='baz',
                   verify=verify).client.get_dataset('qux')

    assert requests_mock.last_request.verify is verify
