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


def _upload_file(client, url, auth, fpath):
    """Send POST request to given URL with file fpath.

    :returns: HTTP response
    """
    with open(fpath, "rb") as test_file:
        response = client.post(
            url,
            input_stream=test_file,
            headers=auth
        )

    return response


def _request_accepted(response):
    """Return True if request was accepted."""
    return response.status_code == 202


@pytest.mark.parametrize(
    "path",
    [
        "test.txt",
        "test_directory/test_file",
        "test_directory/../test_file",
        "tämäontesti.txt",
        "tämä on testi.txt"
    ]
)
def test_upload(app, test_auth, test_mongo, path, requests_mock):
    """Test uploading a plain text file."""
    # Mock metax
    metax_files_api = requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

    test_client = app.test_client()
    files = test_mongo.upload.files
    resolved_path = pathlib.Path('/', path).resolve()
    project_path = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH"),
                                'test_project')

    response = _upload_file(
        test_client, f"/v1/files/test_project/{path}",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 200
    assert response.json['status'] == 'created'
    assert response.json['file_path'] == str(resolved_path)

    # File should be available after metadata has been created
    fpath = project_path / resolved_path.relative_to('/')
    assert fpath.is_file()
    assert fpath.read_bytes() \
        == pathlib.Path("tests/data/test.txt").read_bytes()

    # Check that the file has 664 permissions. The group write
    # permission is required, otherwise siptools-research will crash
    # later.
    assert oct(fpath.stat().st_mode)[5:8] == "664"

    # Check that the uploaded file was added to database
    document = files.find_one({"_id": str(fpath)})
    assert document["checksum"] == "150b62e4e7d58c70503bd5fc8a26463c"
    assert document["identifier"].startswith('urn:uuid:')

    # Check that correct file metadata was sent to Metax
    metadata = metax_files_api.last_request.json()[0]
    assert metadata['file_path'] == str(resolved_path)
    assert metadata['file_name'] == resolved_path.name

    # Test that trying to upload the file again returns 409 Conflict
    response = _upload_file(
        test_client, f"/v1/files/test_project/{path}",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 409
    assert response.json['error'] == f"File '{resolved_path}' already exists"


def test_upload_conflict(test_client, test_auth, requests_mock):
    """Test uploading a file that already has metadata in Metax."""
    # Mock metax
    requests_mock.get(
        '/rest/v2/files?file_path=/foo/bar&project_identifier=test_project',
        json={'results': [{"file_path": '/foo/bar'}], 'next': None}
    )

    # Upload file
    response = _upload_file(
        test_client, "/v1/files/test_project/foo/bar",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 409
    assert response.json['error'] \
        == ('Metadata could not be created because the file already has'
            ' metadata')


def test_upload_max_size(app, test_auth, mock_config):
    """Test uploading file larger than the supported max file size."""
    # Set max upload size to 1 byte
    mock_config["MAX_CONTENT_LENGTH"] = 1
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PROJECTS_PATH")

    response = _upload_file(
        test_client, "/v1/files/test_project/test.txt",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 413
    assert response.json['error'] == "Max single file size exceeded"

    # Check that file was not saved on the server
    fpath = os.path.join(upload_path, "test/test.txt")
    assert not os.path.isfile(fpath)


def test_user_quota(app, test_auth):
    """Test uploading files larger than allowed by user quota."""
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PROJECTS_PATH")

    project = ProjectEntry.objects.get(id="test_project")
    project.quota = 200
    project.used_quota = 0
    project.save()

    response = _upload_file(
        test_client, "/v1/files/test_project/test.zip",
        test_auth, "tests/data/test.zip"
    )
    assert response.status_code == 413
    assert response.json['error'] == "Quota exceeded"

    # Check that the file was not actually created
    assert not os.path.isdir(os.path.join(upload_path,
                                          "test_project",
                                          "test.zip"))


def test_used_quota(app, test_auth, requests_mock):
    """Test that used quota is calculated correctly."""
    # Mock Metax
    requests_mock.get("https://metax.localdomain/rest/v2/files",
                      json={'next': None, 'results': []})
    requests_mock.post("/rest/v2/files/", json={})
    requests_mock.post(
        "https://metax.localdomain/rest/v2/files/datasets?keys=files",
        json=[]
    )
    requests_mock.delete("/rest/v2/files", json={})

    test_client = app.test_client()

    # Upload two 31B txt files
    _upload_file(
        test_client, "/v1/files/test_project/test1",
        test_auth, "tests/data/test.txt"
    )
    _upload_file(
        test_client, "/v1/files/test_project/test2",
        test_auth, "tests/data/test.txt"
    )

    project = ProjectEntry.objects.get(id="test_project")
    assert project.used_quota == 62

    # Delete one of the files
    test_client.delete(
        "/v1/files/test_project/test1",
        headers=test_auth
    )
    project = ProjectEntry.objects.get(id="test_project")
    assert project.used_quota == 31


def test_upload_conflicting_directory(app, test_auth, requests_mock):
    """Test uploading file to path that is a directory."""
    # Mock metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

    # First upload file to "/foo/bar", so that directory "/foo" is
    # created.
    test_client = app.test_client()
    response = _upload_file(
        test_client, "/v1/files/test_project/foo/bar",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 200

    # Then, try to upload file to "/foo". It should fail because
    # directory "/foo" exists
    test_client = app.test_client()
    response = _upload_file(
        test_client, "/v1/files/test_project/foo",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 409
    assert response.json['error'] == "Directory '/foo' already exists"


def test_upload_to_root(app, test_auth):
    """Test uploading file to root directory."""
    test_client = app.test_client()
    response = _upload_file(
        test_client, "/v1/files/test_project/",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 405
    assert response.json['error'] \
        == 'The method is not allowed for the requested URL.'


@pytest.mark.parametrize(
    'method,url,data',
    [
        ("POST", "/v1/files/test_project/../test.txt", b'foo'),
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


def test_unsupported_content(app, test_auth):
    """Test uploading unsupported type."""
    client = app.test_client()

    response = client.post(
        '/v1/files/test_project/test',
        headers=test_auth,
        content_length=1,
        content_type='foo'
    )

    assert response.status_code == 415
    assert response.json['error'] == "Unsupported Content-Type: foo"


def test_unknown_content_length(app, test_auth):
    """Test uploading file without Content-Lengt header."""
    client = app.test_client()

    response = client.post(
        '/v1/files/test_project/test',
        headers=test_auth,
        content_length=None,
        content_type='application/octet-stream',
    )

    assert response.status_code == 411
    assert response.json['error'] == "Missing Content-Length header"


@pytest.mark.parametrize(
    ('checksum', 'expected_status_code', 'expected_response'),
    [
        # The actual md5sum of tests/data/test.txt
        (
            '150b62e4e7d58c70503bd5fc8a26463c',
            200,
            {
                'file_path': '/test_path',
                'status': 'created'
            }

        ),
        # Invalid md5sum
        (
            'foo',
            400,
            {
                'code': 400,
                'error': 'Checksum of uploaded file does not match provided'
                ' checksum.'
            }
        )
    ]
)
def test_file_integrity_validation(app, test_auth, checksum,
                                   expected_status_code,
                                   expected_response, requests_mock):
    """Test integrity validation of uploaded file.

    Upload file with checksum provided in HTTP request header.

    :param app: Flask app
    :param test_auth: authentication headers
    :param cheksum: checksum included in HTTP headers
    :param expected_status_code: expected status of response from API
    :param expected_response: expected JSON response from API
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

    # Post a file
    test_client = app.test_client()
    with open('tests/data/test.txt', "rb") as test_file:
        response = test_client.post(
            '/v1/files/test_project/test_path',
            query_string={'md5': checksum},
            input_stream=test_file,
            headers=test_auth
        )

    # Check response
    assert response.status_code == expected_status_code
    for key in expected_response:

        assert response.json[key] == expected_response[key]


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
def test_delete_file(app, test_auth, requests_mock, test_mongo, name):
    """Test DELETE for single file."""
    # Mock Metax
    requests_mock.get("/rest/v2/files", json={'next': None, 'results': []})
    requests_mock.post("/rest/v2/files/", json={})
    requests_mock.post("/rest/v2/files/datasets", json={})
    delete_files_api = requests_mock.delete("/rest/v2/files", json={})

    # Upload a file
    test_client = app.test_client()
    response = _upload_file(
        test_client, f"/v1/files/test_project/{name}",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 200
    assert response.json['status'] == 'created'

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
    assert request_json[0].startswith('urn:uuid:')

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
                'identifier': 'foo',
                'directories': ['dir2', 'dir1'],
                'files': ['file1.txt']
            }
        ),
        # Directory that contains only files
        (
            '/dir1/',
            {
                'identifier': 'foo',
                'directories': [],
                'files': ['file2.txt']
            }
        ),
        # Directory that contains only directories
        (
            '/dir2/',
            {
                'identifier': 'foo',
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
    shutil.copy(
        "tests/data/test.txt",
        os.path.join(upload_path, "test_project/file1.txt")
    )
    shutil.copy(
        "tests/data/test.txt",
        os.path.join(upload_path, "test_project/dir1/file2.txt")
    )
    shutil.copy(
        "tests/data/test.txt",
        os.path.join(upload_path, "test_project/dir2/subdir1/file3.txt")
    )

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
    app, test_auth, requests_mock, test_mongo, background_job_runner,
    target, files_to_delete
):
    """Test deleting a directory.

    :param target: Directory that will be deleted
    :param files_to_delete: Files that should be deleted when the
                            directory is deleted
    """
    project_directory \
        = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH")) / 'test_project'

    # Mock metax.
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={"results": [], "next": None})
    requests_mock.post("/rest/v2/files/datasets", json={})
    delete_files_api = requests_mock.delete("/rest/v2/files", json={})

    # Create test files
    test_client = app.test_client()
    all_files = ['test.txt', 'test/test.txt']
    all_file_identifiers = {}
    for file in all_files:
        response = _upload_file(test_client,
                                f'/v1/files/test_project/{file}',
                                test_auth,
                                'tests/data/test.txt')
        assert response.status_code == 200
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
        == set(deleted_identifiers)


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
def test_delete_file_in_dataset(test_auth, test_client, requests_mock,
                                path_to_delete):
    """Test deleting a file that belongs to a pending dataset.

    The deletion should fail.

    :param path_to_delete: The path to be deleted.
    """
    # Mock Metax
    requests_mock.get("/rest/v2/files", json={"next": None, "results": []})
    requests_mock.post("/rest/v2/files/", json={})

    # Upload a file
    _upload_file(
        test_client,
        '/v1/files/test_project/testdir/test.txt',
        test_auth,
        'tests/data/test.txt'
    )
    file_id = test_client.get(
        '/v1/files/test_project/testdir/test.txt', headers=test_auth
    ).json['identifier']

    # Create a dataset "test_dataset", and add the uploaded file to it
    requests_mock.post(
        "https://metax.localdomain/rest/datasets/list",
        additional_matcher=(lambda req: req.json() == ["test_dataset"]),
        json={
            "count": 1,
            "results": [
                {
                    "identifier": "test_dataset",
                    "research_dataset": {"title": {"en": "Dataset"}, "files": [{"details": {"project_identifier": "bar"}}]},
                    "preservation_state":
                        DS_STATE_TECHNICAL_METADATA_GENERATED
                }
            ]
        }
    )
    requests_mock.post(
        "https://metax.localdomain/rest/v2/files/datasets",
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
        '/testdir/test.txt',  # only the file
        '/testdir',  # parent directory of the file
        '/',  # project root directory
    ]
)
def test_delete_preserved_file(test_auth, test_client, requests_mock,
                               background_job_runner, path_to_delete):
    """Test deleting a file that belongs to a preserved dataset.

    The file should be deleted, but the metadata should not be deleted
    from Metax (see TPASPKT-749).

    :param path_to_delete: The path to be deleted.
    """
    # Mock Metax
    requests_mock.get("/rest/v2/files", json={"next": None, "results": []})
    requests_mock.post("/rest/v2/files/", json={})
    delete_file_metadata_api = requests_mock.delete("/rest/v2/files", json={})

    # Upload a file
    _upload_file(
        test_client,
        '/v1/files/test_project/testdir/test.txt',
        test_auth,
        'tests/data/test.txt'
    )
    file_id = test_client.get(
        '/v1/files/test_project/testdir/test.txt', headers=test_auth
    ).json['identifier']

    # Create a dataset "test_dataset", and add the uploaded file to it
    requests_mock.post(
        "https://metax.localdomain/rest/datasets/list",
        additional_matcher=lambda req: req.json() == ["test_dataset"],
        json={
            "count": 1,
            "results": [
                {
                    "identifier": "test_dataset",
                    "research_dataset": {"title": {"en": "Dataset"}, "files": [{"details": {"project_identifier": "bar"}}]},
                    "preservation_state": DS_STATE_IN_DIGITAL_PRESERVATION
                }
            ]
        }
    )
    requests_mock.post(
        "https://metax.localdomain/rest/v2/files/datasets",
        additional_matcher=lambda req: req.json() == [file_id],
        json={file_id: ["test_dataset"]}
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

    assert not delete_file_metadata_api.called


@pytest.mark.parametrize(
    ("method", "url"),
    (
        ("GET", "/v1/files/test_project/fake_file"),
        ("POST", "/v1/files/test_project/fake_file"),
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
