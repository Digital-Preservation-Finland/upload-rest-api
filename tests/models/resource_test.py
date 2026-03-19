"""Unit tests for resource module."""
import copy

import pytest
from metax_access import template_data

from tests.utils import update_nested_dict
from upload_rest_api.models.resource import (
    Directory,
    File,
    InvalidPathError,
    Project,
    get_resource,
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


# TODO: According to the docstring, this test should test that paging is
# handled correctly, but it looks it does not test that. Is this test
# necessary?
@pytest.mark.usefixtures('app')  # Initialize db
def test_get_many_datasets(requests_mock):
    """Test that get_datasets method handles paging in Metax."""
    # Create a test file in filesystem
    project = Project.get(id="test_project")
    testfile = project.directory / "testdir" / "testfile"
    testfile.parent.mkdir()
    testfile.write_text("foo")

    # Create file metadata in Metax
    file = copy.deepcopy(template_data.FILE)
    file["pathname"] = "/testdir/testfile"
    requests_mock.get(
        "/v3/files?pathname__startswith=/testdir/&csc_project=test_project",
        json={"next": None, "results": [file]}
    )

    # Add the test file to three datasets: "urn:uuid:dataset1",
    # "urn:uuid:dataset2" and "urn:uuid:dataset3". When the metadata of
    # the datasets is requested from Metax, two datasets are provided
    # per page.
    requests_mock.post(
        "/v3/files/datasets?relations=true",
        json={template_data.FILE["id"]: ["urn:uuid:dataset1",
                                         "urn:uuid:dataset2",
                                         "urn:uuid:dataset3"]}
    )
    datasets = [
        update_nested_dict(
            template_data.DATASET,
            {
                "id": f"urn:uuid:dataset{x}",
                "title": "foo",
                "fileset": {"csc_project": "bar"},
                "preservation": {"state": 10},
            },
        )
        for x in range(1, 4)
    ]
    requests_mock.get(
        "/v3/datasets",
        additional_matcher=lambda req: set(req.qs["id"][0].split(","))
        == {"urn:uuid:dataset1", "urn:uuid:dataset2", "urn:uuid:dataset3"},
        json=datasets,
    )

    # Get the list of datasets associated with directory "test_dir"
    # using get_datasets method. The list should contain both
    # datasets.
    datasets = get_resource('test_project', 'testdir').get_datasets()
    assert {dataset['identifier'] for dataset in datasets} \
        == {'urn:uuid:dataset1', 'urn:uuid:dataset2', 'urn:uuid:dataset3'}


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
