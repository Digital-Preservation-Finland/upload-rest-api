"""Tests for ``upload_rest_api.app`` module."""
import io
import json
import os
import shutil
import time

import pytest
from rq import SimpleWorker

import upload_rest_api.database as database
import upload_rest_api.jobs as jobs


def _contains_symlinks(fpath):
    """Check if fpath or any subdirectories contains symlinks.

    :param fpath: Path to directory to check
    :returns: True if any symlinks are found else False
    """
    for dirpath, _, files in os.walk(fpath):
        for _file in files:
            if os.path.islink("%s/%s" % (dirpath, _file)):
                return True

    return False


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


def _wait_response(test_client, response, test_auth):
    status = "pending"
    polling_url = json.loads(response.data)["polling_url"]
    while status == "pending":
        time.sleep(0.1)
        response = test_client.get(polling_url, headers=test_auth)
        data = json.loads(response.data)
        status = data['status']
    return response, polling_url


def _request_accepted(response):
    """Return True if request was accepted."""
    return response.status_code == 202


def test_index(app, test_auth, wrong_auth):
    """Test the application index page with correct
    and incorrect credentials.
    """
    test_client = app.test_client()

    response = test_client.get("/", headers=test_auth)
    assert response.status_code == 404

    response = test_client.get("/", headers=wrong_auth)
    assert response.status_code == 401


@pytest.mark.usefixtures('mock_config')
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
    assert not os.path.isdir(os.path.join(upload_path, "test_project"))


def test_used_quota(app, test_auth, mock_mongo, requests_mock):
    """Test that used quota is calculated correctly."""
    # Mock Metax
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v1/files?limit=10000&"
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
    ["archive", "dirpath"],
    [
        ("tests/data/test.zip", ""),
        ("tests/data/test.tar.gz", ""),
        ("tests/data/test.tar.gz", "directory"),
        ("tests/data/test.tar.gz", "directory/subdirectory"),
        # TODO: For some reason only relative paths are allowed, so this
        # test case fails
        # ("tests/data/test.tar.gz", "/directory"),
    ]
)
def test_upload_archive(
        archive, dirpath, app, test_auth, mock_mongo, background_job_runner
):
    """Test that uploaded archive is extracted.

    No files should be extracted outside the project directory.

    :param archive: path to test archive
    :param url dirpath: Directory path where archive is extracted
    :param app: Flask app
    :param test_auth: authentication headers
    :param mock_mongo: Mongoclient
    :param background_job_runner: RQ job mocker
    """
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    checksums = mock_mongo.upload.checksums

    url = "/v1/archives?dir={}".format(dirpath) if dirpath else "/v1/archives"
    response = _upload_file(test_client, url, test_auth, archive)
    if _request_accepted(response):
        response = background_job_runner(test_client, "upload", response)
    assert response.status_code == 200

    fpath = os.path.join(upload_path, "test_project", dirpath)
    text_file = os.path.join(fpath, "test", "test.txt")
    archive_file = os.path.join(fpath, os.path.split(archive)[1])

    # test.txt is correctly extracted
    assert os.path.isfile(text_file)
    assert "test" in io.open(text_file, "rt").read()

    # archive file is removed
    assert not os.path.isfile(archive_file)

    # checksum is added to mongo
    assert checksums.count({}) == 1
    checksum = checksums.find_one({"_id": text_file})["checksum"]
    assert checksum == "150b62e4e7d58c70503bd5fc8a26463c"


@pytest.mark.parametrize(
    ["archive", "url"],
    [
        ("tests/data/test.tar.gz", "/v1/archives?dir=dataset"),
        ("tests/data/file1.tar", "/v1/archives?dir=dataset")
    ]
)
def test_upload_archive_overwrite_directory(
        archive, url, app, test_auth, background_job_runner
):
    """Test uploading archive that would overwrite a directory.

    :param archive: path to test archive
    :param url: url where archive is uploaded
    :param app: Flask app
    :param test_auth: authentication headers
    :param background_job_runner: RQ job mocker
    """
    test_client = app.test_client()

    # Upload first archive
    response = _upload_file(test_client, url, test_auth, archive)
    assert response.status_code == 202
    response = background_job_runner(test_client, "upload", response)
    assert response.status_code == 200

    # Trying to upload same archive again should return 409 - Conflict
    response = _upload_file(test_client, url, test_auth, archive)
    data = json.loads(response.data)
    assert response.status_code == 409
    assert data["error"] == "Directory 'dataset' already exists"


@pytest.mark.parametrize(
    ["archive", "url"],
    [
        ("tests/data/test.tar.gz", "/v1/archives")
    ]
)
def test_upload_archive_overwrite_file(
        archive, url, app, test_auth, background_job_runner
):
    """Test uploading archive that would overwrite a file.

    :param archive: path to test archive
    :param url: url where archive is uploaded
    :param app: Flask app
    :param test_auth: authentication headers
    :param background_job_runner: RQ job mocker
    """
    test_client = app.test_client()

    # Upload first archive
    response = _upload_file(test_client, url, test_auth, archive)
    assert response.status_code == 202
    response = background_job_runner(test_client, "upload", response)
    assert response.status_code == 200

    # Trying to upload same archive again should return cause error
    response = _upload_file(test_client, url, test_auth, archive)
    response = background_job_runner(
        test_client, "upload", response, expect_success=False
    )

    data = json.loads(response.data)
    assert response.status_code == 200
    assert data["status"] == "error"
    assert data["errors"][0]["message"] \
        == "File 'test/test.txt' already exists"


@pytest.mark.parametrize(
    ["archive1", "url1", "archive2", "url2"],
    [
        (
            "tests/data/dir1_file1.tar",
            "/v1/archives",
            "tests/data/dir1_file2.tar",
            "/v1/archives"
        ),
        # TODO: For some reason this test case fails
        # (
        #     "tests/data/file1.tar",
        #     "/v1/archives?dir=dir1",
        #     "tests/data/file2.tar",
        #     "/v1/archives?dir=dir1"
        # )
    ]
)
def test_upload_two_archives(
        archive1, url1, archive2, url2, app, test_auth, background_job_runner
):
    """Test uploading two archives to same directory.

    :param archive1: path to first test archive
    :param url1: upload url of first archive
    :param archive2: path to second test archive
    :param url2: url where archive is uploaded
    :param app: Flask app
    :param test_auth: authentication headers
    :param background_job_runner: RQ job mocker
    """
    test_client = app.test_client()

    # Upload first archive
    response = _upload_file(test_client, url1, test_auth, archive1)
    assert response.status_code == 202
    response = background_job_runner(test_client, "upload", response)
    assert response.status_code == 200

    # Upload second archive
    response = _upload_file(test_client, url2, test_auth, archive2)
    response = background_job_runner(
        test_client, "upload", response
    )
    data = json.loads(response.data)
    assert response.status_code == 200
    assert data["status"] == "done"
    assert data["message"] == "Archive uploaded and extracted"


@pytest.mark.parametrize("dirpath", [
    "../",
    "dataset/../../",
    "/dataset"
])
def test_upload_invalid_dir(dirpath, app, test_auth):
    """Test that trying to extract outside the project return 404."""
    test_client = app.test_client()
    response = _upload_file(
        test_client,
        "/v1/archives?dir=%s" % dirpath,
        test_auth,
        "tests/data/test.zip"
    )
    assert response.status_code == 404


def test_upload_archive_concurrent(
        app, test_auth, mock_mongo, background_job_runner
):
    """Test that uploaded archive is extracted.

    No files should be extracted outside the project directory.
    """
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    checksums = mock_mongo.upload.checksums

    response_1 = _upload_file(
        test_client, "/v1/archives", test_auth,
        "tests/data/test.zip"
    )
    response_2 = _upload_file(
        test_client, "/v1/archives", test_auth,
        "tests/data/test2.zip"
    )
    # poll with response's polling_url
    if _request_accepted(response_1):
        polling_url = json.loads(response_1.data)["polling_url"]
        response_1 = background_job_runner(test_client, "upload", response_1)
        data = json.loads(response_1.data)
        assert response_1.status_code == 200
        assert data["status"] == "done"
        assert data["message"] == "Archive uploaded and extracted"

        response_1 = test_client.delete(polling_url, headers=test_auth)
        assert response_1.status_code == 404
        data = json.loads(response_1.data)
        assert data["status"] == "Not found"

    # poll with response's polling_url
    if _request_accepted(response_2):
        polling_url = json.loads(response_2.data)["polling_url"]
        response_2 = background_job_runner(test_client, "upload", response_2)
        data = json.loads(response_2.data)
        assert response_2.status_code == 200
        assert data["status"] == "done"
        assert data["message"] == "Archive uploaded and extracted"

        response_2 = test_client.delete(polling_url, headers=test_auth)
        assert response_2.status_code == 404
        data = json.loads(response_2.data)
        assert data["status"] == "Not found"

    fpath = os.path.join(upload_path, "test_project")

    # test.txt files correctly extracted
    test_text_file = os.path.join(fpath, "test", "test.txt")
    test_2_text_file = os.path.join(fpath, "test2", "test.txt")
    assert os.path.isfile(test_text_file)
    assert "test" in io.open(test_text_file, "rt").read()
    assert os.path.isfile(test_2_text_file)
    assert "test" in io.open(test_2_text_file, "rt").read()

    # archive file is removed
    archive_file1 = os.path.join(fpath,
                                 os.path.split("tests/data/test.zip")[1])
    archive_file2 = os.path.join(fpath,
                                 os.path.split("tests/data/test2.zip")[1])
    assert not os.path.isfile(archive_file1)
    assert not os.path.isfile(archive_file2)

    # checksum is added to mongo
    assert checksums.count() == 2
    checksum = checksums.find_one({"_id": test_text_file})["checksum"]
    assert checksum == "150b62e4e7d58c70503bd5fc8a26463c"
    checksum = checksums.find_one({"_id": test_2_text_file})["checksum"]
    assert checksum == "150b62e4e7d58c70503bd5fc8a26463c"


@pytest.mark.parametrize("archive", [
    "tests/data/symlink.zip",
    "tests/data/symlink.tar.gz"
])
def test_upload_invalid_archive(
        archive, app, test_auth, mock_mongo, background_job_runner):
    """Test that trying to upload a archive with symlinks returns error
    and doesn't create any files.
    """
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    checksums = mock_mongo.upload.checksums

    response = _upload_file(
        test_client, "/v1/archives", test_auth, archive
    )
    if _request_accepted(response):
        response = background_job_runner(
            test_client, "upload", response, expect_success=False
        )

    data = json.loads(response.data)
    assert response.status_code == 200
    assert data["errors"][0]["message"] \
        == "File 'test/link' has unsupported type: SYM"

    fpath = os.path.join(upload_path, "test_project")
    text_file = os.path.join(fpath, "test", "test.txt")
    archive_file = os.path.join(fpath, os.path.split(archive)[1])

    # test.txt is not extracted
    assert not os.path.isfile(text_file)

    # archive file is removed
    assert not os.path.isfile(archive_file)

    # no checksums are added to mongo
    assert checksums.count({}) == 0


def test_upload_file_as_archive(app, test_auth, background_job_runner):
    """Test that trying to upload a file as an archive returns an
    error.
    """
    test_client = app.test_client()

    response = _upload_file(
        test_client, "/v1/archives", test_auth, "tests/data/test.txt"
    )
    if _request_accepted(response):
        response = background_job_runner(
            test_client, "upload", response, expect_success=False
        )

    data = json.loads(response.data)
    assert response.status_code == 400
    assert data["error"] == "Uploaded file is not a supported archive"


def test_get_file(app, test_auth, test2_auth, test3_auth, mock_mongo):
    """Test GET for single file."""
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")

    os.makedirs(os.path.join(upload_path, "test_project"))
    fpath = os.path.join(upload_path, "test_project/test.txt")
    shutil.copy("tests/data/test.txt", fpath)
    mock_mongo.upload.checksums.insert_one({
        "_id": fpath, "checksum": "150b62e4e7d58c70503bd5fc8a26463c"
    })

    # GET file that exists
    response = test_client.get("/v1/files/test.txt", headers=test_auth)

    assert response.status_code == 200
    data = json.loads(response.data)

    assert data["file_path"] == "/test.txt"
    assert data["md5"] == "150b62e4e7d58c70503bd5fc8a26463c"
    assert data["metax_identifier"] == "None"

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
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v1/files?limit=10000&"
                      "project_identifier=test_project",
                      json=response)

    requests_mock.post("https://metax.fd-test.csc.fi/rest/v1/files/datasets",
                       json={})

    requests_mock.delete("https://metax.fd-test.csc.fi/rest/v1/files/foo",
                         json='/test.txt')

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    fpath = os.path.join(upload_path, "test_project/test.txt")

    os.makedirs(os.path.join(upload_path, "test_project"))
    shutil.copy("tests/data/test.txt", fpath)
    mock_mongo.upload.checksums.insert_one({"_id": fpath, "checksum": "foo"})

    # DELETE file that exists
    response = test_client.delete(
        "/v1/files/test.txt",
        headers=test_auth
    )

    assert response.status_code == 200
    assert json.loads(response.data)["metax"] == {'deleted_files_count': 1}
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
        'https://metax.fd-test.csc.fi/rest/v1/directories/files',
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
    data = json.loads(response.data)
    for key in data.keys():
        assert data[key] == expected_data[key] \
            or set(data[key]) == set(expected_data[key])


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
    requests_mock.get('https://metax.fd-test.csc.fi/rest/v1/directories/'
                      'files',
                      status_code=404)

    test_client = app.test_client()
    response = test_client.get("v1/files/test_directory", headers=test_auth)
    assert response.status_code == 200
    assert json.loads(response.data) == {'directories': [],
                                         'files': [],
                                         'identifier': None}


def test_delete_files(
        app, test_auth, requests_mock, mock_mongo, background_job_runner
):
    """Test DELETE for the whole project and a single dir."""
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
            },
            {
                "id": "bar",
                "identifier": "bar",
                "file_path": "/test/test.txt",
                "file_storage": {
                    "identifier": "urn:nbn:fi:att:file-storage-pas"
                }
            }
        ]
    }
    # Mock Metax
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v1/files?limit=10000&"
                      "project_identifier=test_project",
                      json=response)

    requests_mock.post("https://metax.fd-test.csc.fi/rest/v1/files/datasets",
                       json={})

    requests_mock.delete("https://metax.fd-test.csc.fi/rest/v1/files",
                         json=['/test/test.txt'])

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    test_path_1 = os.path.join(upload_path, "test_project/test.txt")
    test_path_2 = os.path.join(upload_path, "test_project/test/test.txt")

    os.makedirs(os.path.join(upload_path, "test_project", "test/"))
    shutil.copy("tests/data/test.txt", test_path_1)
    shutil.copy("tests/data/test.txt", test_path_2)
    checksums = mock_mongo.upload.checksums
    checksums.insert_many([
        {"_id": test_path_1, "checksum": "foo"},
        {"_id": test_path_2, "checksum": "foo"},
    ])

    # DELETE single directory
    response = test_client.delete(
        "/v1/files/test",
        headers=test_auth
    )
    if _request_accepted(response):
        response = background_job_runner(test_client, "files", response)

    assert response.status_code == 200
    assert json.loads(response.data)["message"] \
        == 'Deleted files and metadata: /test'
    assert not os.path.exists(os.path.split(test_path_2)[0])
    assert checksums.count({}) == 1

    # DELETE the whole project
    requests_mock.delete("https://metax.fd-test.csc.fi/rest/v1/files",
                         json=['/test.txt'])
    response = test_client.delete(
        "/v1/files",
        headers=test_auth
    )
    if _request_accepted(response):
        response = background_job_runner(test_client, "files", response)

    assert response.status_code == 200
    assert json.loads(response.data)["message"] \
        == "Deleted files and metadata: /"
    assert not os.path.exists(os.path.split(test_path_1)[0])
    assert checksums.count({}) == 0

    # DELETE project that does not exist
    response = test_client.delete(
        "/v1/files",
        headers=test_auth
    )
    assert response.status_code == 404


def test_delete_metadata(
        app, test_auth, requests_mock, mock_mongo, background_job_runner
):
    """Test DELETE metadata for a directory and a single dir."""
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
            },
            {
                "id": "bar",
                "identifier": "bar",
                "file_path": "/test/test.txt",
                "file_storage": {
                    "identifier": "urn:nbn:fi:att:file-storage-pas"
                }
            }
        ]
    }
    # Mock Metax
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v1/files?limit=10000&"
                      "project_identifier=test_project",
                      json=response)
    requests_mock.post("https://metax.fd-test.csc.fi/rest/v1/files/datasets",
                       json=['dataset_identifier'])
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v1/datasets/"
                      "dataset_identifier",
                      json={"preservation_state": 75})
    adapter = requests_mock.delete(
        "https://metax.fd-test.csc.fi/rest/v1/files",
        json={"deleted_files_count": 1}
    )
    requests_mock.delete("https://metax.fd-test.csc.fi/rest/v1/files/foo",
                         json={})

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    test_path_1 = os.path.join(upload_path, "test_project/test.txt")
    test_path_2 = os.path.join(upload_path, "test_project/test/test.txt")

    os.makedirs(os.path.join(upload_path, "test_project", "test/"))
    shutil.copy("tests/data/test.txt", test_path_1)
    shutil.copy("tests/data/test.txt", test_path_2)
    checksums = mock_mongo.upload.checksums
    checksums.insert_many([
        {"_id": test_path_1, "checksum": "foo"},
        {"_id": test_path_2, "checksum": "foo"},
    ])

    # DELETE metadata for single directory
    response = test_client.delete(
        "/v1/metadata/test",
        headers=test_auth
    )
    if _request_accepted(response):
        response = background_job_runner(test_client, "metadata", response)

    assert response.status_code == 200
    assert json.loads(response.data)["message"] == "1 files deleted"
    assert adapter.last_request.json() == ['bar']

    # DELETE metadata for single file
    response = test_client.delete(
        "/v1/metadata/test.txt",
        headers=test_auth
    )
    if _request_accepted(response):
        response = background_job_runner(test_client, "metadata", response)

    assert response.status_code == 200
    assert json.loads(response.data)["message"] \
        == "1 files deleted"


def test_delete_metadata_dataset_accepted(
        app, test_auth, requests_mock, mock_mongo, background_job_runner
):
    """Test DELETE metadata for a directory and a single file when
    dataset state is accepted to digital preservation.
    """
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
            },
            {
                "id": "bar",
                "identifier": "bar",
                "file_path": "/test/test.txt",
                "file_storage": {
                    "identifier": "urn:nbn:fi:att:file-storage-pas"
                }
            }
        ]
    }
    # Mock Metax
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v1/files?limit=10000&"
                      "project_identifier=test_project",
                      json=response)

    requests_mock.post("https://metax.fd-test.csc.fi/rest/v1/files/datasets",
                       json=['dataset_identifier'])
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v1/datasets/"
                      "dataset_identifier",
                      json={"preservation_state": 80})
    adapter = requests_mock.delete(
        "https://metax.fd-test.csc.fi/rest/v1/files",
        json={"deleted_files_count": 0}
    )
    requests_mock.delete("https://metax.fd-test.csc.fi/rest/v1/files/foo",
                         json={})

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    test_path_1 = os.path.join(upload_path, "test_project/test.txt")
    test_path_2 = os.path.join(upload_path, "test_project/test/test.txt")

    os.makedirs(os.path.join(upload_path, "test_project", "test/"))
    shutil.copy("tests/data/test.txt", test_path_1)
    shutil.copy("tests/data/test.txt", test_path_2)
    checksums = mock_mongo.upload.checksums
    checksums.insert_many([
        {"_id": test_path_1, "checksum": "foo"},
        {"_id": test_path_2, "checksum": "foo"},
    ])

    # DELETE metadata for single directory
    response = test_client.delete(
        "/v1/metadata/test",
        headers=test_auth
    )
    if _request_accepted(response):
        response = background_job_runner(test_client, "metadata", response)

    assert json.loads(response.data)["message"] == "0 files deleted"
    assert adapter.last_request is None

    # DELETE metadata for single file
    response = test_client.delete(
        "/v1/metadata/test.txt",
        headers=test_auth
    )
    if _request_accepted(response):
        response = background_job_runner(
            test_client, "metadata", response, expect_success=False
        )

    response = json.loads(response.data)
    assert response["errors"][0]["message"] \
        == "Metadata is part of an accepted dataset"


def test_post_metadata(app, test_auth, requests_mock, background_job_runner):
    """Test posting file metadata to Metax."""
    test_client = app.test_client()

    # Upload file to test instance
    _upload_file(
        test_client, "/v1/files/foo", test_auth, "tests/data/test.txt"
    )

    # Mock Metax HTTP response
    requests_mock.post(
        "https://metax.fd-test.csc.fi/rest/v1/files/",
        json={"success": [], "failed": ["fail1", "fail2"]}
    )

    response = test_client.post("/v1/metadata/*", headers=test_auth)
    if _request_accepted(response):
        response = background_job_runner(test_client, "metadata", response)

    assert response.status_code == 200
    assert json.loads(response.data) == {
        "message": "Metadata created: /",
        "status": "done"
    }


@pytest.mark.parametrize(
    ('metax_response', 'expected_response'),
    [
        # Path is reserved
        (
            {
                "file_path": ["a file with path /foo already exists in"
                              " project bar"],
            },
            {
                'message': "Task failed",
                'status': 'error',
                'errors': [{
                    'message': "Some of the files already exist.",
                    'files': ['/foo']
                }]
            }
        ),
        # Path and identifier are reserved
        (
            {
                "file_path": ["a file with path /foo already exists in "
                              "project bar"],
                "identifier": ["a file with given identifier already exists"],
            },
            {
                'message': "Task failed",
                'status': 'error',
                'errors': [{
                    'message': "Some of the files already exist.",
                    'files': ['/foo']
                }]
            }
        ),
        # Path is reserved but also unknown error occurs
        (
            {
                "file_path": ["a file with path /foo already exists in "
                              "project bar"],
                "identifier": ["Unknown error"],
            },
            {
                'message': "Internal server error",
                'status': 'error'
            }
        ),
        # Multiple paths are reserved
        (
            {
                "success": [],
                "failed": [
                    {
                        "object": {
                            "file_path": "/foo1",
                            "identifier": "foo1",
                        },
                        "errors": {
                            "file_path": [
                                "a file with path /foo1 already exists in "
                                "project bar"
                            ]
                        }
                    },
                    {
                        "object": {
                            "file_path": "/foo2",
                            "identifier": "foo2",
                        },
                        "errors": {
                            "file_path": [
                                "a file with path /foo2 already exists in "
                                "project bar"
                            ]
                        }
                    }
                ]
            },
            {
                'message': "Task failed",
                'status': 'error',
                'errors': [{
                    'message': "Some of the files already exist.",
                    'files': ['/foo1', '/foo2']
                }]
            }
        )
    ]
)
def test_post_metadata_failure(app, test_auth, requests_mock,
                               background_job_runner, metax_response,
                               expected_response):
    """Test post file metadata failure.

    If posting file metadata to Metax fails, API should return HTTP
    response with status code 200, and the clear error message.

    :param app: Flask application
    :param test_auth: authentication headers
    :param requests_mock: HTTP request mocker
    :param background_job_runner: RQ job mocker
    :param metax_response: Mocked Metax response
    :param expected response: Expected response from API
    """
    test_client = app.test_client()

    # Upload file to test instance
    _upload_file(
        test_client, "/v1/files/foo", test_auth, "tests/data/test.txt"
    )

    # Mock Metax HTTP response
    requests_mock.post("https://metax.fd-test.csc.fi/rest/v1/files/",
                       status_code=400,
                       json=metax_response)

    response = test_client.post("/v1/metadata/foo", headers=test_auth)
    if _request_accepted(response):
        response = background_job_runner(
            test_client, "metadata", response, expect_success=False
        )

    assert response.status_code == 200
    assert json.loads(response.data) == expected_response


def test_reverse_proxy_polling_url(app, test_auth):
    """Mock the web application running behind a reverse proxy and ensure that
    the reverse proxy's URL is detected by the web application.
    """
    test_client = app.test_client()

    # Add an environment variable containing the X-Forwarded-Host HTTP header
    # value.
    # This is how Werkzeug (eg. all WSGI servers) read the HTTP headers for an
    # incoming request.
    response = test_client.post(
        "/v1/metadata/*",
        headers=test_auth,
        environ_base={"HTTP_X_FORWARDED_HOST": "reverse_proxy"}
    )
    polling_url = json.loads(response.data)["polling_url"]

    assert polling_url.startswith("http://reverse_proxy/v1/tasks/")


@jobs.api_background_job
def _modify_task_info(task_id):
    """Modify task info in database."""
    tasks = database.Database().tasks
    tasks.update_message(task_id, "foo")
    tasks.update_status(task_id, "bar")
    return "baz"


@jobs.api_background_job
def _raise_general_exception(task_id):
    """Raise general exception."""
    raise Exception('Something failed')


@jobs.api_background_job
def _raise_client_error(task_id):
    """Raise ClientError."""
    raise jobs.ClientError('Client made mistake.')


@pytest.mark.parametrize(
    ('task_func', 'expected_response'),
    [
        (
            'tests.unit_tests.app_test._modify_task_info',
            {'message': 'baz', 'status': 'done'}
        ),
        (
            'tests.unit_tests.app_test._raise_general_exception',
            {'message': 'Internal server error', 'status': 'error'}
        ),
        (
            'tests.unit_tests.app_test._raise_client_error',
            {
                'message': 'Task failed',
                'errors': [{'message': 'Client made mistake.', 'files': None}],
                'status': 'error'}
        )
    ]
)
def test_query_task(app, mock_redis, test_auth, task_func, expected_response):
    """Test querying task status.

    :param app: Flask app
    :param mock_redis: Redis mocker
    :param test_auth: authentication headers
    :param task_func: function to be queued in RQ
    :param expected_response: expected API JSON response
    """
    # Enqueue a job
    job = jobs.enqueue_background_job(
        task_func=task_func,
        queue_name="upload",
        username="test",
        job_kwargs={}
    )

    # Task should be pending
    test_client = app.test_client()
    response = test_client.get("/v1/tasks/{}".format(job), headers=test_auth)
    assert json.loads(response.data) == {'message': 'processing',
                                         'status': 'pending'}

    # Run job. Task should be finished (status: done) or failed (status:
    # error).
    SimpleWorker([jobs.get_job_queue("upload")],
                 connection=mock_redis).work(burst=True)
    response = test_client.get("/v1/tasks/{}".format(job), headers=test_auth)
    assert json.loads(response.data) == expected_response
