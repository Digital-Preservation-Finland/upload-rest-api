"""Unit tests for metadata generation."""
import pathlib

import pytest

import upload_rest_api.gen_metadata as md


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
