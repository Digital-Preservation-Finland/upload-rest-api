"""Unit tests for resource module."""
import copy

import pytest
from metax_access import template_data

from upload_rest_api.models.resource import (
    Directory,
    File,
    InvalidPathError,
    Project,
)


@pytest.mark.usefixtures('app')  # Initialize database
@pytest.mark.parametrize(
    ("path", "result"),
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
        assert str(File(project_id='test_project', path=path).path) == result
    else:
        with pytest.raises(InvalidPathError):
            File(project_id='test_project', path=path)


@pytest.mark.usefixtures("app")  # Initialize db and create test_project
def test_directory_get_all_files(requests_mock):
    """Test that get_all_files gets expected files.

    Metax has metadata of files that have already been deleted from
    pottumonttu (see TPASPKT-749). This test ensures that metadata of
    deleted files does not affect get_all_dataset (and other methods
    that use private _get_file_group method).
    """
    # Add metadata of three files to Metax
    files = []
    for filename in ["test1", "test2", "test3"]:
        file = copy.deepcopy(template_data.FILE)
        file["pathname"] = "/testdir/" + filename
        files.append(file)
    requests_mock.get(
        "/v3/files?pathname__startswith=/testdir/",
        json={
            "next": None,
            "results": files,
        }
    )

    # Create two files storage directory. So the third file only has
    # metadata Metax, but it does not exist in upload-rest-api
    testdir = Project.get(id="test_project").directory / "testdir"
    testdir.mkdir()
    existing_file_paths = [testdir / "test1", testdir / "test2"]
    for file in existing_file_paths:
        file.touch()

    # get_all_files should get only "test1" and "test2", although
    # metadata of "test3" still exists in Metax
    directory = Directory(path="testdir", project_id="test_project")
    files = directory.get_all_files()
    assert [file.storage_path for file in files] == existing_file_paths
