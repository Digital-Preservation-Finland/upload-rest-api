"""Tests for ``upload_rest_api.api.v1.files`` module."""
import copy
import pathlib
import shutil

import pytest
from metax_access import (
    DS_STATE_IN_DIGITAL_PRESERVATION,
    DS_STATE_TECHNICAL_METADATA_GENERATED,
    template_data,
)

from tests.utils import update_nested_dict
from upload_rest_api.lock import LockAlreadyTaken, ProjectLockManager
from upload_rest_api.models.project import Project, ProjectEntry


def test_used_quota(test_client, test_auth, tus_upload_file, requests_mock):
    """Test that used quota is calculated correctly."""
    # Mock Metax
    requests_mock.get("/v3/files?pathname=%2Ftest1&csc_project=test_project",
                      json={'next': None, 'results': []})
    requests_mock.get("/v3/files?pathname=%2Ftest2&csc_project=test_project",
                      json={'next': None, 'results': []})
    requests_mock.post("/v3/files/post-many", json={})
    requests_mock.post(
        "/v3/files/datasets?relations=true",
        json={}
    )
    requests_mock.post("/v3/files/delete-many", json={})

    # Upload two 31B txt files
    tus_upload_file(project_id="test_project", path="test1")
    tus_upload_file(project_id="test_project", path="test2")

    project = ProjectEntry.objects.get(id="test_project")
    assert project.used_quota == 62

    # Manually update Metax mockup before deleting /test1. Metadata for
    # /test1 should be available, because it has been uploaded.
    test1 = copy.deepcopy(template_data.FILE)
    test1["pathname"] = "/test1"
    requests_mock.get("/v3/files?pathname=%2Ftest1&csc_project=test_project",
                      json={"next": None, "results": [test1]})

    # Delete one of the files
    test_client.delete(
        "/v1/files/test_project/test1",
        headers=test_auth
    )
    project = ProjectEntry.objects.get(id="test_project")
    assert project.used_quota == 31


@pytest.mark.parametrize(
    ("method", "url", "data"),
    [
        ("GET", "/v1/files/test_project/../test.txt", None),
        ("DELETE", "/v1/files/test_project/../test.txt", None),
    ]
)
def test_resource_outside_project_directory(app, method, url, data, test_auth):
    """Test accessing resource outside project directory.

    :param url: URL of request
    :param data: content of Request
    """
    client = app.test_client()
    response = client.open(url, data=data, headers=test_auth, method=method)

    assert response.status_code == 400
    assert response.json['error'] == 'Invalid path'


def test_get_file(app, test_auth, test_auth2, requests_mock):
    """Test GET for single file."""
    test_client = app.test_client()

    # Create a file in test_project
    upload_path = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH"))
    fpath = upload_path / "test_project/test.txt"
    shutil.copy("tests/data/test.txt", fpath)

    #  Mock metax
    file = copy.deepcopy(template_data.FILE)
    file["pathname"] = "/test.txt"
    file["checksum"] = "150b62e4e7d58c70503bd5fc8a26463c"
    file["id"] = "fake_identifier"
    requests_mock.get(
        "/v3/files?pathname=%2Ftest.txt&csc_project=test_project",
        json={"next": None, "results": [file]}
    )

    # GET file that exists
    response = test_client.get(
        "/v1/files/test_project/test.txt", headers=test_auth
    )

    assert response.status_code == 200
    assert response.json["file_path"] == "/test.txt"
    assert response.json["md5"] == "150b62e4e7d58c70503bd5fc8a26463c"
    assert response.json["identifier"] == "fake_identifier"

    # GET file with user that is not in "test_project"
    response = test_client.get(
        "/v1/files/test_project/test.txt", headers=test_auth2
    )
    assert response.status_code == 403
    assert response.json['error'] == 'No permission to access this project'


@pytest.mark.parametrize(
    "name", ["test.txt", "tämäontesti.txt", "tämä on testi.txt"]
)
def test_delete_file(app, test_auth, requests_mock, name):
    """Test DELETE for single file."""
    # Mock Metax
    file = copy.deepcopy(template_data.FILE)
    file["pathname"] = f"/{name}"
    file["id"] = "test-identifier"
    requests_mock.get(
        f"/v3/files?pathname=%2F{name}&csc_project=test_project",
        json={"next": None, "results": [file]}
    )
    requests_mock.post("/v3/files/datasets?relations=true", json={})
    delete_files_api = requests_mock.post("/v3/files/delete-many", json={})

    # Add file to filesystem
    fpath = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH")) \
        / "test_project" / name
    fpath.write_text("foo")
    assert fpath.exists()

    # DELETE file that exists
    test_client = app.test_client()
    response = test_client.delete(
        f"/v1/files/test_project/{name}",
        headers=test_auth
    )
    assert response.status_code == 200
    assert response.json["metax"] == {'deleted_files_count': 1}

    # Check that file was removed from filesystem
    assert not fpath.exists()

    # Check that file was removed from Metax
    assert delete_files_api.called_once
    request_json = delete_files_api.last_request.json()
    assert isinstance(request_json, list)
    assert len(request_json) == 1
    assert request_json[0]["id"] == file["id"]

    # Try to DELETE file that does not exist. Request should fail with
    # 404 "Not found" error.
    response = test_client.delete(
        f"/v1/files/test_project/{name}",
        headers=test_auth
    )
    assert response.status_code == 404
    assert response.json['error'] == 'File not found'


@pytest.mark.parametrize(
    ("path", "expected_data"),
    [
        # Root directory, contains files and directories
        (
            '/',
            {
                'directories': ['dir2', 'dir1'],
                'files': ['file1.txt']
            }
        ),
        # Directory that contains only files
        (
            '/dir1/',
            {
                'directories': [],
                'files': ['file2.txt']
            }
        ),
        # Directory that contains only directories
        (
            '/dir2/',
            {
                'directories': ['subdir1'],
                'files': []
            }
        )
    ]
)
def test_get_files(app, test_auth, path, expected_data, requests_mock):
    """Test GET for directories.

    :param app: Flask app
    :param test_auth: authentication headers
    :param path: directory path to be tested
    :param expected_data: expected response data
    :param requests_mock: HTTP request mocker
    """
    # Create sample directory structure
    upload_path = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH"))
    (upload_path / "test_project/dir1").mkdir(parents=True)
    (upload_path / "test_project/dir2/subdir1").mkdir(parents=True)
    (upload_path / "test_project/file1.txt").touch()
    (upload_path / "test_project/dir1/file2.txt").touch()

    # Mock metax
    file1 = copy.deepcopy(template_data.FILE)
    file1["filename"] = "file1.txt"
    file1["pathname"] = "/file1.txt"
    file2 = copy.deepcopy(template_data.FILE)
    file2["filename"] = "file2.txt"
    file2["pathname"] = "/dir1/file2.txt"
    requests_mock.get(
        "/v3/directories?path=/",
        json={
            "next": None,
            "results": {
                # "directory" not used, but required by metax_access
                "directory": {"pathname": ""},
                # "directories" not used, but required by metax_access
                "directories": [],
                "files": [file1],
            }
        }
    )
    requests_mock.get(
        "/v3/directories?path=/dir1",
        json={
            "next": None,
            "results": {
                "directory": {"pathname": ""},
                "directories": [],
                "files": [file2],
            }
        }
    )
    requests_mock.get(
        "/v3/directories?path=/dir2",
        json={
            "next": None,
            "results": {
                "directory": {"pathname": ""},
                "directories": [],
                "files": [],
            }
        }
    )

    # Check the response
    test_client = app.test_client()
    response = test_client.get(
        "/v1/files/test_project" + path, headers=test_auth
    )
    assert response.status_code == 200
    for key in response.json:
        assert response.json[key] == expected_data[key] \
            or set(response.json[key]) == set(expected_data[key])


@pytest.mark.parametrize(
    ("target", "files_to_delete"),
    [
        ("/test", ["/test/test.txt"]),
        ("/", ["/test.txt", "/test/test.txt"]),
        ("", ["/test.txt", "/test/test.txt"])
    ]
)
def test_delete_directory(
    app, test_auth, requests_mock,
    background_job_runner, target, files_to_delete
):
    """Test deleting a directory.

    :param target: Directory that will be deleted
    :param files_to_delete: Files that should be deleted when the
                            directory is deleted
    """
    project_directory \
        = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH")) / "test_project"

    # Mock metax.
    requests_mock.post("/v3/files/datasets?relations=true", json={})
    delete_files_api = requests_mock.post("/v3/files/delete-many", json={})

    # Create test files
    for file in ("test.txt", "test/test.txt"):
        absolute_path = project_directory / file
        absolute_path.parent.mkdir(exist_ok=True)
        absolute_path.write_text("foo")

    file1 = copy.deepcopy(template_data.FILE)
    file1["pathname"] = "/test.txt"
    file1["id"] = "id1"
    file2 = copy.deepcopy(template_data.FILE)
    file2["pathname"] = "/test/test.txt"
    file2["id"] = "id2"
    requests_mock.get(
        "/v3/files?csc_project=test_project&pathname__startswith=/",
        json={
            "next": None,
            "results": [file1, file2]
        }
    )
    requests_mock.get(
        "/v3/files?csc_project=test_project&pathname__startswith=/test/",
        json={
            "next": None,
            "results": [file2],
        }
    )

    # Delete a directory. Deletion taks should be created and directory
    # should be locked
    test_client = app.test_client()
    response = test_client.delete(
        f"/v1/files/test_project{target}", headers=test_auth
    )
    assert response.status_code == 202
    assert response.json['message'] == 'Deleting metadata'
    assert response.json['status'] == 'pending'
    with pytest.raises(LockAlreadyTaken):
        ProjectLockManager().acquire(
            'test_project', project_directory / target.strip('/')
        )

    # Check that tasks API return correct message after directory has
    # been deleted
    response = background_job_runner(test_client, "files", response)
    assert response.status_code == 200
    assert response.json["message"] \
        == f'Deleted files and metadata: /{target.strip("/")}'

    # The files in target directory should be deleted. Other files
    # should still exist.
    for file in (file1, file2):
        storage_path = project_directory / file["pathname"].strip("/")
        if file["pathname"] in files_to_delete:
            assert not storage_path.exists()
        else:
            assert storage_path.exists()

    # The target directory and subdirectories should be deleted. Project
    # directory should still exist.
    assert not (project_directory / 'test').exists()
    assert project_directory.exists()

    # Expected files should have been deleted from Metax
    assert delete_files_api.called_once
    ids_of_deleted_files = {
        file["id"]
        for file
        in delete_files_api.request_history[0].json()
    }
    ids_of_files_that_should_be_deleted = {
        file["id"]
        for file
        in (file1, file2)
        if file["pathname"] in files_to_delete
    }
    assert ids_of_deleted_files == ids_of_files_that_should_be_deleted


def test_delete_empty_project(app, test_auth, requests_mock):
    """Test DELETE for project that does not have any files."""
    # Mock Metax
    requests_mock.get(
        "/v3/files?pathname__startswith=/",
        json={
            "next": None,
            "results": [],
        }
    )

    test_client = app.test_client()

    # Try to delete project
    response = test_client.delete("/v1/files/test_project", headers=test_auth)
    assert response.status_code == 404
    assert response.json['error'] == "No files found"


@pytest.mark.parametrize(
    'path_to_delete',
    [
        '/testdir/test.txt',  # only the file
        '/testdir',  # parent directory of the file
        '/',  # project root directory
    ]
)
def test_delete_file_in_dataset(
        test_auth, test_client, requests_mock, path_to_delete
):
    """Test deleting a file that belongs to a pending dataset.

    The deletion should fail.

    :param path_to_delete: The path to be deleted.
    """
    # Create a test file
    test_file_path = Project.get(id="test_project").directory \
        / "testdir" / "test.txt"
    test_file_path.parent.mkdir(parents=True)
    test_file_path.write_text("foo")
    # Mock Metax
    file_md = copy.deepcopy(template_data.FILE)
    file_md["pathname"] = "/testdir/test.txt"
    file_md["id"] = "test-id-1"
    requests_mock.get(
        "/v3/files?pathname=/testdir/test.txt&csc_project=test_project",
        json={"next": None, "results": [file_md]}
    )
    requests_mock.get(
        "/v3/files?pathname__startswith=/testdir/&csc_project=test_project",
        json={"next": None, "results": [file_md]}
    )
    requests_mock.get(
        "/v3/files?pathname__startswith=/&csc_project=test_project",
        json={"next": None, "results": [file_md]}
    )

    # Create a dataset "test_dataset", and add the uploaded file to it
    requests_mock.get(
        "/v3/datasets?id=test_dataset",
        json=[
                update_nested_dict(
                    template_data.DATASET,
                    {
                        "id": "test_dataset",
                        "title": {"en": "Dataset"},
                        "fileset": {"csc_project": "bar"},
                        "preservation": {
                            "state": DS_STATE_TECHNICAL_METADATA_GENERATED
                        },
                    },
                )
            ]
    )
    requests_mock.post(
        "/v3/files/datasets?relations=true",
        additional_matcher=(
            lambda req: req.json() == ["test-id-1"]
        ),
        json={"test-id-1": ["test_dataset"]}
    )

    # Try to delete the file (or some parent directory of the file)
    response = test_client.delete(
        f"/v1/files/test_project{path_to_delete}", headers=test_auth
    )
    assert response.status_code == 400
    assert response.json["error"] == \
        "File/directory is used in a pending dataset and cannot be deleted"


@pytest.mark.parametrize(
    'path_to_delete',
    [
        '/testdir/test1.txt',  # only the file
        '/testdir',  # parent directory of the file
        '/',  # project root directory
    ]
)
def test_delete_preserved_file(
    test_auth, test_client, requests_mock, background_job_runner,
    path_to_delete
):
    """Test deleting a file that belongs to a preserved dataset.

    The file should be deleted, but the metadata should not be deleted
    from Metax (see TPASPKT-749).

    :param path_to_delete: The path to be deleted.
    """
    # Create two files. test1.txt will be added to a dataset, and the
    # metadata of this file will NOT be removed from Metax. test2.txt is
    # not added to any dataset, so its metadata is removed from Metax if
    # the file is removed.
    project_directory = Project.get(id="test_project").directory
    (project_directory / "testdir").mkdir()
    file1_path = project_directory / "testdir" / "test1.txt"
    file1_path.write_text("contenct of text1.txt")
    file2_path = project_directory / "testdir" / "test2.txt"
    file2_path.write_text("contenct of text2.txt")

    # Mock Metax
    file1 = copy.deepcopy(template_data.FILE)
    file1["pathname"] = "/testdir/test1.txt"
    file1["id"] = "file1-id"
    file2 = copy.deepcopy(template_data.FILE)
    file2["pathname"] = "/testdir/test2.txt"
    file2["id"] = "file2-id"
    requests_mock.get(
        "/v3/files?pathname=/testdir/test1.txt&csc_project=test_project",
        json={"next": None, "results": [file1]}
    )
    requests_mock.get(
        "/v3/files?pathname__startswith=/testdir/&csc_project=test_project",
        json={"next": None, "results": [file1, file2]}
    )
    requests_mock.get(
        "/v3/files?pathname__startswith=/&csc_project=test_project",
        json={"next": None, "results": [file1, file2]}
    )
    delete_file_metadata_api \
        = requests_mock.post("/v3/files/delete-many", json={})

    # Create a dataset "test_dataset", and add test1.txt to it
    requests_mock.get(
        "/v3/datasets?id=test_dataset",
        json=[
                update_nested_dict(
                    template_data.DATASET,
                    {
                        "id": "test_dataset",
                        "title": {"en": "Dataset"},
                        "fileset": {"csc_project": "bar"},
                        "preservation": {
                            "state": DS_STATE_IN_DIGITAL_PRESERVATION
                        },
                    },
                )
            ]
    )
    requests_mock.post(
        "/v3/files/datasets?relations=true",
        json={
            file1["id"]: ["test_dataset"],
            file2["id"]: []
        }
    )

    # Delete the file (or some parent directory of the file)
    response = test_client.delete(
        f"/v1/files/test_project{path_to_delete}", headers=test_auth
    )
    if response.status_code == 202:
        # Directory deletion task queued. Run the background task.
        response = background_job_runner(test_client, "files", response)
        assert response.json['status'] == 'done'
        assert response.json["message"] \
            == f"Deleted files and metadata: {path_to_delete}"
    elif response.status_code == 200:
        # Single file deleted
        assert response.json["file_path"] == path_to_delete
        assert response.json["message"] == "deleted"
    else:
        raise ValueError('Wrong status code')

    if pathlib.Path("/testdir/text2.txt").is_relative_to(path_to_delete):
        # Both files are removed from upload-rest-api. The metadata of
        # test2.txt should be removed from Metax, as it is not in
        # preservation.
        assert delete_file_metadata_api.called_once
        assert delete_file_metadata_api.request_history[0].json() == [
            {
                "id": file2["id"],
            }
        ]
    else:
        # Only test1.txt is removed removed from upload-rest-api.
        # Metadata should not be removed from Metax, because test1.txt
        # is in digital preservation.
        assert not delete_file_metadata_api.called


@pytest.mark.parametrize(
    ("method", "url"),
    [
        ("GET", "/v1/files/test_project/fake_file"),
        ("DELETE", "/v1/files/test_project/fake_file"),
        # Access project that does not exist
        ("GET", "/v1/files/nonexistent_project/fake_file"),
    ]
)
def test_no_rights(test_auth2, test_client, method, url):
    """
    Test that attempting to access a project without permission results
    in a 403 Forbidden response
    """
    response = test_client.open(url, method=method, headers=test_auth2)

    assert response.status_code == 403


def test_file_size_limit(test_client, test_auth):
    """Test accessing the file size limit."""
    response = test_client.get("/v1/files/get_size_limit", headers=test_auth)
    assert response.status_code == 200
    assert response.json["file_size_limit"] == 50 * 1024**3
