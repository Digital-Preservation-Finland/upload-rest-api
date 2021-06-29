"""Tests for ``upload_rest_api.api.v1.files`` module."""
import os
import pathlib
import shutil

import pytest


def _set_user_quota(users, username, quota, used_quota):
    """Set quota and used quota of user username."""
    users.update_one({"_id": username}, {"$set": {"quota": quota}})
    users.update_one({"_id": username}, {"$set": {"used_quota": used_quota}})


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


def test_incorrect_authentication(app, wrong_auth):
    """Test index page with incorrect authentication credentials."""
    response = app.test_client().get("/", headers=wrong_auth)
    assert response.status_code == 401


def test_upload(app, test_auth, mock_mongo):
    """Test uploading a plain text file."""
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    checksums = mock_mongo.upload.checksums

    response = _upload_file(
        test_client, "/v1/files/test.txt",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 200

    fpath = os.path.join(upload_path, "test_project/test.txt")
    assert os.path.isfile(fpath)
    assert open(fpath, "rb").read() == open("tests/data/test.txt", "rb").read()

    # Check that the uploaded files checksum was added to mongo
    checksum = checksums.find_one({"_id": fpath})["checksum"]
    assert checksum == "150b62e4e7d58c70503bd5fc8a26463c"

    # Test that trying to upload the file again returns 409 Conflict
    response = _upload_file(
        test_client, "/v1/files/test.txt",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 409


def test_upload_max_size(app, test_auth):
    """Test uploading file larger than the supported max file size."""
    # Set max upload size to 1 byte
    app.config["MAX_CONTENT_LENGTH"] = 1
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")

    response = _upload_file(
        test_client, "/v1/files/test.txt",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 413

    # Check that file was not saved on the server
    fpath = os.path.join(upload_path, "test/test.txt")
    assert not os.path.isfile(fpath)


def test_user_quota(app, test_auth, mock_mongo):
    """Test uploading files larger than allowed by user quota."""
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    users = mock_mongo.upload.users

    _set_user_quota(users, "test", 200, 0)
    response = _upload_file(
        test_client, "/v1/files/test.zip",
        test_auth, "tests/data/test.zip"
    )
    assert response.status_code == 413

    # Check that the file was not actually created
    assert not os.path.isdir(os.path.join(upload_path,
                                          "test_project",
                                          "test.zip"))


def test_used_quota(app, test_auth, mock_mongo, requests_mock):
    """Test that used quota is calculated correctly."""
    # Mock Metax
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v2/files?limit=10000&"
                      "project_identifier=test_project",
                      json={'next': None, 'results': []})

    test_client = app.test_client()
    users = mock_mongo.upload.users

    # Upload two 31B txt files
    _upload_file(
        test_client, "/v1/files/test1",
        test_auth, "tests/data/test.txt"
    )
    _upload_file(
        test_client, "/v1/files/test2",
        test_auth, "tests/data/test.txt"
    )
    used_quota = users.find_one({"_id": "test"})["used_quota"]
    assert used_quota == 62

    # Delete one of the files
    test_client.delete(
        "/v1/files/test1",
        headers=test_auth
    )
    used_quota = users.find_one({"_id": "test"})["used_quota"]
    assert used_quota == 31


def test_upload_outside(app, test_auth):
    """Test uploading outside the user's dir."""
    test_client = app.test_client()
    response = _upload_file(
        test_client, "/v1/files/../test.txt",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 404


@pytest.mark.parametrize(
    ('checksum', 'expected_status_code', 'expected_response'),
    [
        # The actual md5sum of tests/data/test.txt
        (
            '150b62e4e7d58c70503bd5fc8a26463c',
            200,
            {
                'file_path': '/test_path',
                'md5': '150b62e4e7d58c70503bd5fc8a26463c',
                'status': 'created',
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
                                   expected_response):
    """Test integrity validation of uploaded file.

    Upload file with checksum provided in HTTP request header.

    :param app: Flask app
    :param test_auth: authentication headers
    :param cheksum: checksum included in HTTP headers
    :param expected_status_code: expected status of response from API
    :param expected_response: expected JSON response from API
    """
    # Post archive
    test_client = app.test_client()
    with open('tests/data/test.txt', "rb") as test_file:
        response = test_client.post(
            '/v1/files/test_path',
            query_string={'md5': checksum},
            input_stream=test_file,
            headers=test_auth
        )

    # Check response
    assert response.status_code == expected_status_code
    for key in expected_response:
        assert response.json[key] == expected_response[key]


def test_get_file(app, test_auth, test2_auth, test3_auth, mock_mongo):
    """Test GET for single file."""
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")

    fpath = os.path.join(upload_path, "test_project/test.txt")
    shutil.copy("tests/data/test.txt", fpath)
    mock_mongo.upload.checksums.insert_one({
        "_id": fpath, "checksum": "150b62e4e7d58c70503bd5fc8a26463c"
    })

    # GET file that exists
    response = test_client.get("/v1/files/test.txt", headers=test_auth)

    assert response.status_code == 200

    assert response.json["file_path"] == "/test.txt"
    assert response.json["md5"] == "150b62e4e7d58c70503bd5fc8a26463c"
    assert response.json["identifier"] is None

    # GET file with user test2, which is in the same project
    response = test_client.get("/v1/files/test.txt", headers=test2_auth)
    assert response.status_code == 200

    # GET file with user test3, which is not in the same project
    response = test_client.get("/v1/files/test.txt", headers=test3_auth)
    assert response.status_code == 404


def test_delete_file(app, test_auth, requests_mock, mock_mongo):
    """Test DELETE for single file."""
    response = {
        "next": None,
        "results": [
            {
                "id": "foo",
                "identifier": "foo",
                "file_path": "/test.txt",
                "file_storage": {
                    "identifier": "urn:nbn:fi:att:file-storage-pas"
                }
            }
        ]
    }
    # Mock Metax
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v2/files?limit=10000&"
                      "project_identifier=test_project",
                      json=response)

    requests_mock.post("https://metax.fd-test.csc.fi/rest/v2/files/datasets",
                       json={})

    requests_mock.delete("https://metax.fd-test.csc.fi/rest/v2/files/foo",
                         json='/test.txt')

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    fpath = os.path.join(upload_path, "test_project/test.txt")

    shutil.copy("tests/data/test.txt", fpath)
    mock_mongo.upload.checksums.insert_one({"_id": fpath, "checksum": "foo"})

    # DELETE file that exists
    response = test_client.delete(
        "/v1/files/test.txt",
        headers=test_auth
    )

    assert response.status_code == 200
    assert response.json["metax"] == {'deleted_files_count': 1}
    assert not os.path.isfile(fpath)
    assert mock_mongo.upload.checksums.count({}) == 0

    # DELETE file that does not exist
    response = test_client.delete(
        "/v1/files/test.txt",
        headers=test_auth
    )
    assert response.status_code == 404


@pytest.mark.parametrize(
    ['path', 'expected_data'],
    [
        # All files of user
        (
            '',
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
    :param data: expected response data
    """
    requests_mock.get(
        'https://metax.fd-test.csc.fi/rest/v2/directories/files',
        json={'identifier': 'foo', 'directories': []}
    )

    # Create sample directory structure
    upload_path = app.config.get("UPLOAD_PATH")
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
    response = test_client.get("/v1/files" + path, headers=test_auth)
    assert response.status_code == 200
    for key in response.json.keys():
        assert response.json[key] == expected_data[key] \
            or set(response.json[key]) == set(expected_data[key])


def test_get_directory_without_identifier(app, test_auth, requests_mock):
    """Test listing contents of directory without identifier.

    Response should list directory contents, but the directory
    identifier should be missing.

    :param app: Flask app
    :param test_auth: authentication headers
    """
    # Create test directory
    upload_path = app.config.get("UPLOAD_PATH")
    os.makedirs(os.path.join(upload_path, "test_project/test_directory"))

    # Metax responds with 404, which means that test directory metadata
    # does not (yet) exist in Metax.
    requests_mock.get('https://metax.fd-test.csc.fi/rest/v2/directories/'
                      'files',
                      status_code=404)

    test_client = app.test_client()
    response = test_client.get("v1/files/test_directory", headers=test_auth)
    assert response.status_code == 200
    assert response.json == {'directories': [],
                             'files': [],
                             'identifier': None}


@pytest.mark.parametrize('target', ['/test', '/'])
def test_delete_directory(
    app, test_auth, requests_mock, mock_mongo, background_job_runner, target
):
    """Test deleting a directory."""
    # Create test data
    test_client = app.test_client()

    _upload_file(test_client,
                 '/v1/files/test.txt',
                 test_auth,
                 'tests/data/test.txt')

    _upload_file(test_client,
                 '/v1/files/test/test.txt',
                 test_auth,
                 'tests/data/test.txt')

    # Find the target files
    project_directory \
        = pathlib.Path(app.config.get("UPLOAD_PATH")) / 'test_project'
    target_files = list()
    for root, _, files in os.walk(project_directory / target.strip('/')):
        target_files += [pathlib.Path(root) / file_ for file_ in files]

    # Mock Metax
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v2/files?limit=10000&"
                      "project_identifier=test_project",
                      json={'results': [], 'next': None})

    requests_mock.post("https://metax.fd-test.csc.fi/rest/v2/files/datasets",
                       json={})

    requests_mock.delete("https://metax.fd-test.csc.fi/rest/v2/files",
                         json=target_files)

    # Delete a directory
    response = test_client.delete(f"/v1/files{target}", headers=test_auth)
    if _request_accepted(response):
        response = background_job_runner(test_client, "files", response)
    assert response.status_code == 200
    assert response.json["message"] \
        == f'Deleted files and metadata: {target}'

    # The files in target directory should be deleted. Other files
    # should still exist.
    for file_ in [project_directory / 'test.txt',
                  project_directory / 'test/test.txt']:
        if file_ in target_files:
            assert not file_.exists()
            assert not mock_mongo.upload.checksums.find_one(
                {"_id": str(file_)}
            )
        else:
            assert file_.exists()
            assert mock_mongo.upload.checksums.find_one({"_id": str(file_)})

    # The target directory and subdirectories should be deleted. Other
    # directories should still exist.
    for directory in ['/test', '/']:
        path = project_directory / directory.strip('/')
        assert directory.startswith(target) is not path.exists()


def test_delete_empty_project(app, test_auth, requests_mock,
                              background_job_runner):
    """Test DELETE for project that does not have any files."""
    # Mock Metax
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v2/files?limit=10000&"
                      "project_identifier=test_project",
                      json={"next": None, "results": []})

    test_client = app.test_client()

    # DELETE project
    response = test_client.delete("/v1/files", headers=test_auth)
    if _request_accepted(response):
        response = background_job_runner(test_client, "files", response)
    assert response.status_code == 200

    # DELETE project that does not exist
    response = test_client.delete("/v1/files", headers=test_auth)
    assert response.status_code == 404
