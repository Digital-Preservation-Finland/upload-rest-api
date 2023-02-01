"""Unit tests for resource module."""
import io

import pytest

from upload_rest_api.models.resource import (File, InvalidPathError,
                                             get_resource)
from upload_rest_api.models.upload import Upload


@pytest.mark.usefixtures('app')  # Initialize database
@pytest.mark.parametrize(
    "path,result",
    [
        # Valid
        ("/", "/"),
        ("/test", "/test"),
        ("test", "/test"),
        ("/test/../taste", "/taste"),
        ("/test/../test/../test", "/test"),
        ("/äö/😂", "/äö/😂"),
        ("/🐸/🐸🐸/🐸🐸🐸/..", "/🐸/🐸🐸"),
        ("/test/..", "/"),

        # Invalid
        ("../test", None),
        ("/test/../../", None),
    ]
)
def test_parse_relative_user_path(path, result):
    """Test valid and invalid user provided relative paths.

    Ensure valid paths result in the given relative path, while invalid
    paths raise an exception.
    """
    if result is not None:
        assert str(File('test_project', path).path) == result
    else:
        with pytest.raises(InvalidPathError):
            File('test_project', path)


@pytest.mark.usefixtures('app')  # Initialize db
def test_get_many_datasets(requests_mock):
    """Test that get_datasets method handles paging in Metax."""
    # Mock metax
    requests_mock.get('/rest/v2/files', json={'results': []})
    requests_mock.post('/rest/v2/files/', json={})

    # Upload a file to directory /testdir
    upload = Upload.create(File('test_project', 'testdir/testfile'), 123)
    with io.BytesIO(b'foo') as textfile:
        upload.add_source(file=textfile, checksum=None)
    upload.store_files(verify_source=False)
    file_identifier \
        = get_resource('test_project', 'testdir/testfile').identifier

    # Add the uploaded file to three datasets: "urn:uuid:dataset1",
    # "urn:uuid:dataset2" and "urn:uuid:dataset3". When the metadata of
    # the datasets is requested from Metax, two datasets are provided
    # per page.
    requests_mock.post(
        "/rest/v2/files/datasets?keys=files",
        json={file_identifier: ["urn:uuid:dataset1",
                                "urn:uuid:dataset2",
                                "urn:uuid:dataset3"]}
    )
    requests_mock.post(
        "/rest/datasets/list?offset=0&limit=1000000"
        "&fields=identifier%2Cpreservation_state%2Cresearch_dataset",
        json={
            "count": 3,
            "results": [
                {
                    "identifier": "urn:uuid:dataset1",
                    "research_dataset": {'title': 'foo'},
                    "preservation_state": 10
                },
                {
                    "identifier": "urn:uuid:dataset2",
                    "research_dataset": {'title': 'foo'},
                    "preservation_state": 10
                }
            ],
        }
    )
    requests_mock.post(
        "/rest/datasets/list?offset=2&limit=1000000"
        "&fields=identifier%2Cpreservation_state%2Cresearch_dataset",
        json={
            "count": 3,
            "results": [
                {
                    "identifier": "urn:uuid:dataset3",
                    "research_dataset": {'title': 'foo'},
                    "preservation_state": 10
                }
            ],
        }
    )

    # Get the list of datasets associated with directory "test_dir"
    # using get_datasets method. The list should contain both
    # datasets.
    datasets = get_resource('test_project', 'testdir').get_datasets()
    assert {dataset['identifier'] for dataset in datasets} \
        == {'urn:uuid:dataset1', 'urn:uuid:dataset2', 'urn:uuid:dataset3'}
