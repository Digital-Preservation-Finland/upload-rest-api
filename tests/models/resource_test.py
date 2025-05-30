"""Unit tests for resource module."""
import io

import pytest

from upload_rest_api.models.resource import (File, InvalidPathError,
                                             get_resource)
from upload_rest_api.models.upload import Upload
from tests.metax_data.utils import TEMPLATE_FILE, TEMPLATE_DATASET, update_nested_dict

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
    requests_mock.get('/v3/files?pathname=%2Ftestdir%2Ftestfile&csc_project=test_project&include_nulls=True',
                    json={'next': None, 'results': []})
    requests_mock.post('/v3/files/post-many?include_nulls=True', json={})

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
        "/v3/files/datasets?relations=true&include_nulls=True",
        json={file_identifier: ["urn:uuid:dataset1",
                                "urn:uuid:dataset2",
                                "urn:uuid:dataset3"]}
    )
    ds = {
        "title": "foo",
        "fileset": {"csc_project": "bar"},
        "preservation": {"state": 10},
    }
    ds['id'] = "urn:uuid:dataset1"
    requests_mock.get(
        "/v3/datasets/urn:uuid:dataset1?include_nulls=True",
        json = update_nested_dict(TEMPLATE_DATASET,
                                  ds)
    )
    ds['id'] = "urn:uuid:dataset2"
    requests_mock.get(
        "/v3/datasets/urn:uuid:dataset2?include_nulls=True",
        json = update_nested_dict(TEMPLATE_DATASET,
                                  ds)
    )
    ds['id'] = "urn:uuid:dataset3"
    requests_mock.get(
        "/v3/datasets/urn:uuid:dataset3?include_nulls=True",
        json = update_nested_dict(TEMPLATE_DATASET,
                                  ds)
    )

    # Get the list of datasets associated with directory "test_dir"
    # using get_datasets method. The list should contain both
    # datasets.
    datasets = get_resource('test_project', 'testdir').get_datasets()
    assert {dataset['identifier'] for dataset in datasets} \
        == {'urn:uuid:dataset1', 'urn:uuid:dataset2', 'urn:uuid:dataset3'}
