"""Tests for ``upload_rest_api.api.v1.files`` module."""
import os
import pathlib
import shutil

import pytest
import pymongo
from metax_access import (DS_STATE_TECHNICAL_METADATA_GENERATED,
                          DS_STATE_IN_DIGITAL_PRESERVATION)

from upload_rest_api.models.file_entry import FileEntry
from upload_rest_api.models.project import ProjectEntry
from upload_rest_api.lock import ProjectLockManager, LockAlreadyTaken
from tests.utils import update_nested_dict
from metax_access.template_data import FILE as TEMPLATE_FILE
from metax_access.template_data import DATASET as TEMPLATE_DATASET


def _request_accepted(response):
    """Return True if request was accepted."""
    return response.status_code == 202


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

    # Delete one of the files
    test_client.delete(
        "/v1/files/test_project/test1",
        headers=test_auth
    )
    project = ProjectEntry.objects.get(id="test_project")
    assert project.used_quota == 31


@pytest.mark.parametrize(
    'method,url,data',
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


def test_get_file(app, test_auth, test_auth2):
    """Test GET for single file."""
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PROJECTS_PATH")

    fpath = os.path.join(upload_path, "test_project/test.txt")
    shutil.copy("tests/data/test.txt", fpath)
    FileEntry(
        path=fpath, checksum="150b62e4e7d58c70503bd5fc8a26463c",
        identifier="fake_identifier"
    ).save()

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
    "name", ("test.txt", "tämäontesti.txt", "tämä on testi.txt")
)
def test_delete_file(
        app, test_auth, tus_upload_file, requests_mock, test_mongo, name):
    """Test DELETE for single file."""
    # Mock Metax
    requests_mock.get("/v3/files?pathname=%2Ftest.txt&csc_project=test_project",
                      json={'next': None, 'results': []})
    requests_mock.get("/v3/files?pathname=%2Ft%C3%A4m%C3%A4ontesti.txt&csc_project=test_project",
                      json={'next': None, 'results': []})
    requests_mock.get("/v3/files?pathname=%2Ft%C3%A4m%C3%A4+on+testi.txt&csc_project=test_project",
                      json={'next': None, 'results': []})
    requests_mock.post("/v3/files/post-many", json={})
    requests_mock.post("/v3/files/datasets?relations=true", json={})
    delete_files_api = requests_mock.post("/v3/files/delete-many", json={})

    # Upload a file
    test_client = app.test_client()
    tus_upload_file(project_id="test_project", path=name)

    # DELETE file that exists
    response = test_client.delete(
        f"/v1/files/test_project/{name}",
        headers=test_auth
    )
    assert response.status_code == 200
    assert response.json["metax"] == {'deleted_files_count': 1}

    # Check that file was removed from filesystem
    fpath = os.path.join(app.config.get("UPLOAD_PROJECTS_PATH"),
                         "test_project", name)
    assert not os.path.isfile(fpath)

    # TODO remove support for pymongo 3.x when RHEL9 migration is done
    if pymongo.__version__ < "3.7":
        assert test_mongo.upload.files.count({}) == 0
    else:
        # Check that file was removed from database
        assert test_mongo.upload.files.count_documents({}) == 0

    # Check that file was removed from Metax
    assert delete_files_api.called_once
    request_json = delete_files_api.last_request.json()
    assert isinstance(request_json, list)
    assert len(request_json) == 1
    assert request_json[0]['storage_identifier'].startswith('urn:uuid:')

    # Try to DELETE file that does not exist. Request should fail with
    # 404 "Not found" error.
    response = test_client.delete(
        f"/v1/files/test_project/{name}",
        headers=test_auth
    )
    assert response.status_code == 404
    assert response.json['error'] == 'File not found'


@pytest.mark.parametrize(
    ['path', 'expected_data'],
    [
        # All files of user
        (
            '/?all=true',
            {
                '/': ['file1.txt'],
                '/dir1': ['file2.txt'],
                '/dir2': [],
                '/dir2/subdir1': ['file3.txt']
            }
        ),
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
    requests_mock.get(
        'https://metax.localdomain/rest/v2/directories/files',
        json={'identifier': 'foo', 'directories': []}
    )

    # Create sample directory structure
    upload_path = app.config.get("UPLOAD_PROJECTS_PATH")
    os.makedirs(os.path.join(upload_path, "test_project/dir1"))
    os.makedirs(os.path.join(upload_path, "test_project/dir2/subdir1"))

    for path_ in ["file1.txt", "dir1/file2.txt", "dir2/subdir1/file3.txt"]:
        shutil.copy(
            "tests/data/test.txt",
            os.path.join(upload_path, "test_project", path_)
        )
        FileEntry(
            path=os.path.join(upload_path, "test_project", path_),
            checksum="foo",
            identifier=path_
        ).save()

    # Check the response
    test_client = app.test_client()
    response = test_client.get(
        "/v1/files/test_project" + path, headers=test_auth
    )
    assert response.status_code == 200
    for key in response.json.keys():
        assert response.json[key] == expected_data[key] \
            or set(response.json[key]) == set(expected_data[key])


@pytest.mark.parametrize(
    'target,files_to_delete',
    [
        ('/test', ['test/test.txt']),
        ('/', ['test.txt', 'test/test.txt']),
        ('', ['test.txt', 'test/test.txt'])
    ]
)
def test_delete_directory(
    app, tus_upload_file, test_auth, requests_mock, test_mongo,
    background_job_runner, target, files_to_delete
):
    """Test deleting a directory.

    :param target: Directory that will be deleted
    :param files_to_delete: Files that should be deleted when the
                            directory is deleted
    """
    project_directory \
        = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH")) / 'test_project'

    # Mock metax.
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files?pathname=%2Ftest.txt&csc_project=test_project',
                      json={"results": [], "next": None})
    requests_mock.get('/v3/files?pathname=%2Ftest%2Ftest.txt&csc_project=test_project',
                      json={"results": [], "next": None})
    requests_mock.get('/v3/files?pathname=%2Ftestdir%2Ftest.txt&csc_project=test_project',
                      json={"results": [], "next": None})
    requests_mock.post("/v3/files/datasets?relations=true&storage_service=pas", json={})
    delete_files_api = requests_mock.post("/v3/files/delete-many", json={})

    # Create test files
    test_client = app.test_client()
    all_files = ['test.txt', 'test/test.txt']
    all_file_identifiers = {}
    for file in all_files:
        tus_upload_file(project_id="test_project", path=file)
        all_file_identifiers[file] = test_client.get(
            f'/v1/files/test_project/{file}', headers=test_auth
        ).json['identifier']

    # Delete a directory. Deletion taks should be created and directory
    # should be locked
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
    for file in all_files:
        storage_path = project_directory / file
        if file in files_to_delete:
            assert not storage_path.exists()
            assert not test_mongo.upload.files.find_one(
                {"_id": str(storage_path)}
            )
        else:
            assert storage_path.exists()
            assert test_mongo.upload.files.find_one({"_id": str(storage_path)})

    # The target directory and subdirectories should be deleted. Project
    # directory should still exist.
    assert not (project_directory / 'test').exists()
    assert project_directory.exists()

    # Expected files should have been deleted from Metax
    assert delete_files_api.called_once
    deleted_identifiers = delete_files_api.request_history[0].json()
    assert {all_file_identifiers[file] for file in files_to_delete} \
        == set([d['storage_identifier'] for d in deleted_identifiers])


def test_delete_empty_project(app, test_auth, requests_mock):
    """Test DELETE for project that does not have any files."""
    # Mock Metax
    requests_mock.post(
        "https://metax.localdomain/rest/v2/files/datasets?keys=files",
        json=[]
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
        test_auth, test_client, tus_upload_file,
        requests_mock, path_to_delete):
    """Test deleting a file that belongs to a pending dataset.

    The deletion should fail.

    :param path_to_delete: The path to be deleted.
    """
    # Mock Metax
    requests_mock.get("/v3/files?pathname=%2Ftestdir%2Ftest.txt&csc_project=test_project", json={"next": None, "results": []})
    requests_mock.post("/v3/files/post-many", json={})

    # Upload a file
    tus_upload_file(project_id="test_project", path="testdir/test.txt")
    file_id = test_client.get(
        '/v1/files/test_project/testdir/test.txt', headers=test_auth
    ).json['identifier']

    # Create a dataset "test_dataset", and add the uploaded file to it
    requests_mock.get(
        "/v3/datasets?id=test_dataset",
        json=[
                update_nested_dict(
                    TEMPLATE_DATASET,
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
        additional_matcher=lambda req: req.json() == [file_id],
        json={file_id: ["test_dataset"]}
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
        test_auth, test_client, tus_upload_file, requests_mock,
        background_job_runner, path_to_delete):
    """Test deleting a file that belongs to a preserved dataset.

    The file should be deleted, but the metadata should not be deleted
    from Metax (see TPASPKT-749).

    :param path_to_delete: The path to be deleted.
    """
    # Mock Metax
    requests_mock.get("/v3/files?pathname=%2Ftestdir%2Ftest1.txt&csc_project=test_project",
                      json={"next": None, "results": []})
    requests_mock.get("/v3/files?pathname=%2Ftestdir%2Ftest2.txt&csc_project=test_project",
                      json={"next": None, "results": []})
    requests_mock.post("/v3/files/post-many", json={})
    delete_file_metadata_api \
        = requests_mock.post("/v3/files/delete-many", json={})

    # Upload a file that will be added to a dataset. The metadata of
    # this file should NOT be removed from Metax.
    tus_upload_file(project_id="test_project", path="testdir/test1.txt")
    file_id1 = test_client.get(
        '/v1/files/test_project/testdir/test1.txt', headers=test_auth
    ).json['identifier']

    # Upload a file that is not added to any dataset. The metadata of
    # this file should be removed from Metax if the file is removed.
    tus_upload_file(project_id="test_project", path="testdir/test2.txt")
    file_id2 = test_client.get(
        '/v1/files/test_project/testdir/test2.txt', headers=test_auth
    ).json['identifier']

    # Create a dataset "test_dataset", and add test1.txt to it
    requests_mock.get(
        "/v3/datasets?id=test_dataset",
        json=[
                update_nested_dict(
                    TEMPLATE_DATASET,
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
        json={file_id1: ["test_dataset"], file_id2: []}
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
                "storage_identifier": file_id2,
                "storage_service": "pas",
            }
        ]
    else:
        # Only test1.txt is removed removed from upload-rest-api.
        # Metadata should not be removed from Metax, because test1.txt
        # is in digital preservation.
        assert not delete_file_metadata_api.called


@pytest.mark.parametrize(
    ("method", "url"),
    (
        ("GET", "/v1/files/test_project/fake_file"),
        ("DELETE", "/v1/files/test_project/fake_file"),
    )
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
