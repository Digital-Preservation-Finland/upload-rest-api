"""Tests for ``upload_rest_api.api.v1.files`` module."""
import json
import os
import pathlib
import shutil

import pytest
from metax_access import DS_STATE_TECHNICAL_METADATA_GENERATED

from upload_rest_api.models import FileEntry, ProjectEntry


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


def test_index(app, test_auth):
    """Test the application index page."""
    response = app.test_client().get("/", headers=test_auth)
    assert response.status_code == 404
    assert response.json['error'] == 'Page not found'


def test_incorrect_authentication(app, wrong_auth):
    """Test index page with incorrect authentication credentials."""
    response = app.test_client().get("/", headers=wrong_auth)
    assert response.status_code == 401


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


def test_get_file(app, test_auth, test2_auth, test3_auth):
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

    # GET file with user test2, which is in the same project
    response = test_client.get(
        "/v1/files/test_project/test.txt", headers=test2_auth
    )
    assert response.status_code == 200

    # GET file with user test3, which is not in the same project
    response = test_client.get(
        "/v1/files/test_project/test.txt", headers=test3_auth
    )
    assert response.status_code == 403
    assert response.json['error'] == 'No permission to access this project'


@pytest.mark.parametrize(
    "name", ("test.txt", "tämäontesti.txt", "tämä on testi.txt")
)
def test_delete_file(app, test_auth, requests_mock, test_mongo, name):
    """Test DELETE for single file."""
    response = {
        "next": None,
        "results": [
            {
                "id": "foo",
                "identifier": "foo",
                "file_path": f"/{name}",
                "file_storage": {
                    "identifier": "urn:nbn:fi:att:file-storage-pas"
                }
            }
        ]
    }
    # Mock Metax
    requests_mock.get("https://metax.localdomain/rest/v2/files?limit=10000&"
                      "project_identifier=test_project",
                      json=response)

    requests_mock.post("https://metax.localdomain/rest/v2/files/datasets",
                       json={})

    requests_mock.delete("https://metax.localdomain/rest/v2/files/foo",
                         json='/test.txt')

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PROJECTS_PATH")
    fpath = os.path.join(upload_path, "test_project", name)

    shutil.copy("tests/data/test.txt", fpath)
    test_mongo.upload.files.insert_one({"_id": fpath, "checksum": "foo"})

    # DELETE file that exists
    response = test_client.delete(
        f"/v1/files/test_project/{name}",
        headers=test_auth
    )

    assert response.status_code == 200
    assert response.json["metax"] == {'deleted_files_count': 1}
    assert not os.path.isfile(fpath)
    assert test_mongo.upload.files.count({}) == 0

    # DELETE file that does not exist
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
        ('/test', ['/test/test.txt']),
        ('/', ['/test.txt', '/test/test.txt']),
        ('', ['/test.txt', '/test/test.txt'])
    ]
)
def test_delete_directory(
    app, test_auth, requests_mock, test_mongo, background_job_runner,
    upload_tmpdir, target, files_to_delete
):
    """Test deleting a directory."""
    # Mock metax. First, there is no file metadata available.
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={"results": [], "next": None})

    # Create test data
    test_client = app.test_client()

    response = _upload_file(test_client,
                            '/v1/files/test_project/test.txt',
                            test_auth,
                            'tests/data/test.txt')

    response = _upload_file(test_client,
                            '/v1/files/test_project/test/test.txt',
                            test_auth,
                            'tests/data/test.txt')

    # After creating the test data, each file has metadata in Metax.
    requests_mock.get(
        "https://metax.localdomain/rest/v2/files?limit=10000&"
        "project_identifier=test_project",
        json={
            'results': [
                {
                    "id": f"id-{file_}",
                    "identifier": f"identifier-{file_}",
                    "file_path": file_,
                    "file_storage": {
                        "identifier": "urn:nbn:fi:att:file-storage-pas"
                    }
                }
                for file_ in files_to_delete
            ],
            'next': None
        }
    )

    # Find the target files
    project_directory \
        = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH")) / 'test_project'
    target_files = []
    for root, _, files in os.walk(project_directory / target.strip('/')):
        target_files += [pathlib.Path(root) / file_ for file_ in files]

    # File don't belong to any dataset
    requests_mock.post("https://metax.localdomain/rest/v2/files/datasets",
                       json={})

    requests_mock.delete("https://metax.localdomain/rest/v2/files",
                         json=[str(file_) for file_ in target_files])

    # Delete a directory
    response = test_client.delete(
        f"/v1/files/test_project{target}", headers=test_auth
    )

    # The directory to delete has been moved to a temporary location to
    # perform the actual file and metadata deletion.
    assert not (project_directory / 'test').exists()

    trash_dir = upload_tmpdir / "trash"
    trash_files = list(trash_dir.glob("*/test_project/**/*.txt"))
    assert len(trash_files) == len(files_to_delete)
    for file_path in files_to_delete:
        assert any(
            True for path in trash_files if str(path).endswith(file_path)
        )

    if _request_accepted(response):
        response = background_job_runner(test_client, "files", response)
    assert response.status_code == 200
    assert response.json["message"] \
        == f'Deleted files and metadata: /{target.strip("/")}'

    # The temporary directory has been deleted
    assert trash_dir.exists()
    assert len(list(trash_dir.iterdir())) == 0

    # The files in target directory should be deleted. Other files
    # should still exist.
    for file_ in [project_directory / 'test.txt',
                  project_directory / 'test' / 'test.txt']:
        if file_ in target_files:
            assert not file_.exists()
            assert not test_mongo.upload.files.find_one({"_id": str(file_)})
        else:
            assert file_.exists()
            assert test_mongo.upload.files.find_one({"_id": str(file_)})

    # The target directory and subdirectories should be deleted. Project
    # directory should still exist.
    assert not (project_directory / 'test').exists()
    assert project_directory.exists()

    # Files should have been deleted from Metax
    delete_request = next(
        request for request in requests_mock.request_history
        if request.method == "DELETE"
        and request.url.endswith("/rest/v2/files")
    )
    deleted_identifiers = delete_request.json()

    # All named files were deleted
    for file_ in files_to_delete:
        assert f"identifier-{file_}" in deleted_identifiers


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


def test_delete_file_in_dataset(
        test_auth, test_client, requests_mock, test_mongo, upload_tmpdir):
    """Test DELETE for a file that belongs to a pending dataset.

    The deletion should fail.
    """
    requests_mock.get(
        "https://metax.localdomain/rest/v2/files"
        "?file_path=/test.txt&project_identifier=test_project",
        json={
            "next": None,
            "previous": None,
            "results": []
        }
    )

    requests_mock.post(
        "https://metax.localdomain/rest/v2/files/",
        additional_matcher=(
            lambda req: json.loads(req.body)[0]["file_path"] == "/test.txt"
        ),
        json={
            "success": [
                {
                    "object": {
                        "identifier": "test_file",
                        "file_path": "/test.txt",
                        "parent_directory": {"identifier": "parent_dir"},
                        "checksum": {"value": ""}
                    }
                }
            ],
            "failed": []
        }
    )

    _upload_file(
        test_client,
        '/v1/files/test_project/test.txt',
        test_auth,
        'tests/data/test.txt'
    )

    # Set the identifier directly instead of mocking the Metax process
    test_mongo.upload.files.update_one(
        {"_id": str(upload_tmpdir / "projects" / "test_project" / "test.txt")},
        {"$set": {"identifier": "test_file"}}
    )

    requests_mock.post(
        "https://metax.localdomain/rest/v2/files/datasets",
        additional_matcher=lambda req: json.loads(req.body) == ["test_file"],
        json={
            "test_file": ["test_dataset"]
        }
    )
    requests_mock.post(
        "https://metax.localdomain/rest/datasets/list",
        additional_matcher=(
            lambda req: json.loads(req.body) == ["test_dataset"]
        ),
        json={
            "next": None,
            "previous": None,
            "count": 1,
            "results": [
                {
                    "identifier": "test_dataset",
                    "research_dataset": {
                        "title": {"en": "Dataset"},
                        "language": [
                            {"identifier": "http://lexvo.org/id/iso639-3/eng"}
                        ]
                    },
                    "preservation_state":
                        DS_STATE_TECHNICAL_METADATA_GENERATED
                }
            ]
        }
    )

    response = test_client.delete(
        "/v1/files/test_project/test.txt", headers=test_auth
    )

    assert response.status_code == 400
    assert response.json["error"] == \
        "File/directory is used in a pending dataset and cannot be deleted"


@pytest.mark.parametrize(
    ("method", "url"),
    (
        ("GET", "/v1/files/test_project/fake_file"),
        ("POST", "/v1/files/test_project/fake_file"),
        ("DELETE", "/v1/files/test_project/fake_file"),
    )
)
def test_no_rights(user2_token_auth, test_client, method, url):
    """
    Test that attempting to access a project without permission results
    in a 403 Forbidden response
    """
    response = test_client.open(url, method=method, headers=user2_token_auth)

    assert response.status_code == 403
