"""Tests for ``upload_rest_api.app`` module."""
import os
import pathlib
import filecmp

import pytest


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
    "archive", ["tests/data/test.zip", "tests/data/test.tar.gz"]
)
def test_upload_archive(
    archive, app, test_auth, test_mongo, background_job_runner, requests_mock
):
    """Test uploading archive.

    Test that:

    * API response contains correct message
    * Files are extracted to correct location
    * Archive file is removed after extraction
    * Files in archive are added to database

    :param archive: path to test archive
    :param app: Flask app
    :param test_auth: authentication headers
    :param test_mongo: Mongoclient
    :param background_job_runner: RQ job mocker
    :param request_mock: HTTP request mocker
    """
    # Mock metax
    metax_files_api = requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'next': None, 'results': []})

    test_client = app.test_client()
    files = test_mongo.upload.files

    response = _upload_file(test_client,
                            "/v1/archives/test_project",
                            test_auth,
                            archive)
    assert response.status_code == 202
    assert response.json['file_path'] == '/'
    assert response.json["message"] == "Uploading archive"
    assert response.json["polling_url"].startswith(
        'http://localhost/v1/tasks/'
    )
    assert response.json["status"] == "pending"
    assert response.headers['Location'] == response.json["polling_url"]

    # archive is first saved to temporary directory as source file
    upload_tmp_path = pathlib.Path(app.config.get("UPLOAD_TMP_PATH"))
    tmp_dirs = [path for path in upload_tmp_path.iterdir() if path.is_dir()]
    assert len(tmp_dirs) == 1
    assert filecmp.cmp(tmp_dirs[0] / 'source', archive)

    # Complete the task and check task status
    response = background_job_runner(test_client, "upload", response)
    assert response.status_code == 200
    assert response.json['status'] == 'done'
    assert response.json['message'] == 'archive uploaded to /'

    # test.txt is correctly extracted
    text_file = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH")) \
        / "test_project" / "test" / "test.txt"
    assert text_file.is_file()
    assert "test" in text_file.read_text()

    # Archive file is removed. Tmp directory should be empty.
    assert not any(upload_tmp_path.iterdir())

    # file is added to database
    assert files.count({}) == 1
    document = files.find_one({"_id": str(text_file)})
    assert document['checksum'] == "150b62e4e7d58c70503bd5fc8a26463c"
    assert document['identifier'].startswith('urn:uuid:')

    # correct metadata is sent to Metax
    metadata = metax_files_api.last_request.json()[0]
    assert metadata['file_path'] == '/test/test.txt'
    assert metadata['file_name'] == 'test.txt'


@pytest.mark.parametrize(
    "dirpath",
    [
        "directory",
        "directory/subdirectory",
        "/directory",
        "///directory",
        "directory/"
    ]
)
def test_upload_archive_to_dirpath(
        dirpath, app, test_auth, background_job_runner, requests_mock
):
    """Test that archive is extracted to path given as parameter.

    :param dirpath: Directory path where archive is extracted
    :param app: Flask app
    :param test_auth: authentication headers
    :param background_job_runner: RQ job mocker
    :param requests_mock: HTTP request mocker
    """
    # Mock metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'next': None, 'results': []})

    test_client = app.test_client()

    url = f"/v1/archives/test_project?dir={dirpath}"
    response \
        = _upload_file(test_client, url, test_auth, 'tests/data/test.tar.gz')
    assert response.status_code == 202
    assert response.json['file_path'] == f"/{dirpath.strip('/')}"

    # Complete the task
    response = background_job_runner(test_client, "upload", response)
    assert response.status_code == 200
    assert response.json['status'] == 'done'
    assert response.json['message'] \
        == f'archive uploaded to /{dirpath.strip("/")}'

    # test.txt is correctly extracted
    text_file = (pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH"))
                 / 'test_project' / dirpath.lstrip('/')
                 / "test" / "test.txt")
    assert text_file.is_file()
    assert "test" in text_file.read_text()


def test_upload_archive_already_exists(
        test_client, test_auth, requests_mock
):
    """Test uploading archive to path that is a file.

    :param test_client: Flask test client
    :param test_auth: authentication headers
    :param requests_mock: HTTP request mocker
    """
    # Mock metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'next': None, 'results': []})

    # Upload a file to a path that will cause a conflict
    url = '/v1/files/test_project/foo'
    _upload_file(test_client, url, test_auth, "tests/data/test.txt")

    # Try to upload an archive to same path
    response = _upload_file(test_client,
                            '/v1/archives/test_project?dir=foo',
                            test_auth,
                            'tests/data/test.tar.gz')
    assert response.status_code == 409
    assert response.json['error'] == "File '/foo' already exists"
    assert response.json['files'] == ['/foo']


@pytest.mark.parametrize(
    ["archive1", "url1", "archive2", "url2"],
    [
        (
            "tests/data/dir1_file1.tar",
            "/v1/archives/test_project",
            "tests/data/dir1_file2.tar",
            "/v1/archives/test_project"
        ),
        (
            "tests/data/file1.tar",
            "/v1/archives/test_project?dir=dir1",
            "tests/data/file2.tar",
            "/v1/archives/test_project?dir=dir1"
        )
    ]
)
def test_upload_two_archives(
    archive1, url1, archive2, url2, app, test_auth, background_job_runner,
    requests_mock
):
    """Test uploading two archives to same directory.

    :param archive1: path to first test archive
    :param url1: upload url of first archive
    :param archive2: path to second test archive
    :param url2: url where archive is uploaded
    :param app: Flask app
    :param test_auth: authentication headers
    :param background_job_runner: RQ job mocker
    :param requests_mock: HTTP request mocker
    """
    # Mock metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'next': None, 'results': []})

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
    assert response.status_code == 200
    assert response.json["status"] == "done"


@pytest.mark.parametrize("dirpath", [
    "../",
    "dataset/../../",
    "/../",
    "///../"
])
def test_upload_invalid_dir(dirpath, app, test_auth):
    """Test that trying to extract outside the project return 404."""
    test_client = app.test_client()
    response = _upload_file(
        test_client,
        f"/v1/archives/test_project?dir={dirpath}",
        test_auth,
        "tests/data/test.zip"
    )
    assert response.status_code == 400
    assert response.json['error'] == "Invalid path"


def test_upload_archive_multiple_archives(
        app, test_auth, test_mongo, background_job_runner, requests_mock
):
    """Test that uploaded archive is extracted.

    No files should be extracted outside the project directory.

    :param app: Flask app
    :param test_auth: authentication headers
    :param test_mongo: mongo client
    :param background_job_runner: RQ job mocker
    :param requests_mock: HTTP request mocker
    """
    # Mock metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'next': None, 'results': []})

    test_client = app.test_client()
    upload_path = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH"))
    files = test_mongo.upload.files

    response_1 = _upload_file(
        test_client, "/v1/archives/test_project", test_auth,
        "tests/data/test.zip"
    )
    # poll with response's polling_url
    if _request_accepted(response_1):
        polling_url = response_1.json["polling_url"]
        response_1 = background_job_runner(test_client, "upload", response_1)
        assert response_1.status_code == 200
        assert response_1.json["status"] == "done"
        assert response_1.json["message"] == "archive uploaded to /"

        response_1 = test_client.delete(polling_url, headers=test_auth)
        assert response_1.status_code == 404
        assert response_1.json["status"] == "Not found"

    response_2 = _upload_file(
        test_client, "/v1/archives/test_project", test_auth,
        "tests/data/test2.zip"
    )
    # poll with response's polling_url
    if _request_accepted(response_2):
        polling_url = response_2.json["polling_url"]
        response_2 = background_job_runner(test_client, "upload", response_2)
        assert response_2.status_code == 200
        assert response_2.json["status"] == "done"
        assert response_2.json["message"] == "archive uploaded to /"

        response_2 = test_client.delete(polling_url, headers=test_auth)
        assert response_2.status_code == 404
        data = response_2.json
        assert data["status"] == "Not found"

    fpath = upload_path / "test_project"

    # test.txt files correctly extracted
    test_text_file = fpath / "test" / "test.txt"
    test_2_text_file = fpath / "test2" / "test.txt"
    assert test_text_file.is_file()
    assert "test" in test_text_file.read_text()
    assert test_2_text_file.is_file()
    assert "test" in test_2_text_file.read_text()

    # archive file is removed
    archive_file1 = fpath / os.path.split("tests/data/test.zip")[1]
    archive_file2 = fpath / os.path.split("tests/data/test2.zip")[1]
    assert not archive_file1.is_file()
    assert not archive_file2.is_file()

    # files are added to mongo
    assert files.count() == 2
    assert files.find_one({"_id": str(test_text_file)})
    assert files.find_one({"_id": str(test_2_text_file)})


@pytest.mark.parametrize("archive", [
    "tests/data/symlink.zip",
    "tests/data/symlink.tar.gz"
])
def test_upload_invalid_archive(
        archive, app, test_auth, test_mongo, background_job_runner):
    """Test uploading invalid archive.

    Test that trying to upload a archive with symlinks returns error and
    doesn't create any files.
    """
    test_client = app.test_client()
    upload_path = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH"))
    files = test_mongo.upload.files

    response = _upload_file(
        test_client, "/v1/archives/test_project", test_auth, archive
    )
    if _request_accepted(response):
        response = background_job_runner(
            test_client, "upload", response, expect_success=False
        )

    assert response.status_code == 200
    assert response.json["errors"][0]["message"] \
        == "File 'test/link' has unsupported type: SYM"

    fpath = upload_path / "test_project"
    text_file = fpath / "test" / "test.txt"
    archive_file = fpath / os.path.split(archive)[1]

    # test.txt is not extracted
    assert not text_file.is_file()

    # archive file is removed
    assert not archive_file.is_file()

    # no files are added to mongo
    assert files.count({}) == 0


def test_upload_large_archive(app, test_auth, mock_config):
    """Test uploading too large archive."""
    mock_config["MAX_CONTENT_LENGTH"] = 1

    response = _upload_file(app.test_client(),
                            '/v1/archives/test_project',
                            test_auth,
                            'tests/data/test.tar.gz')

    assert response.status_code == 413
    assert response.json['error'] == 'Max single file size exceeded'


def test_upload_unsupported_content(app, test_auth):
    """Test uploading unsupported content type."""
    response = app.test_client().post('/v1/archives/test_project',
                                      headers=test_auth,
                                      content_length='1',
                                      content_type='foo')

    assert response.status_code == 415
    assert response.json['error'] == "Unsupported Content-Type: foo"


def test_upload_unknown_content_length(app, test_auth):
    """Test uploading archive without Content-Length header."""
    response = app.test_client().post('/v1/archives/test_project',
                                      headers=test_auth,
                                      content_length=None,
                                      content_type="application/octet-stream")

    assert response.status_code == 411
    assert response.json['error'] == "Missing Content-Length header"


def test_upload_blank_tar(app, test_auth, background_job_runner):
    """Test that trying to upload a blank tar file returns an error."""
    test_client = app.test_client()

    response = _upload_file(
        test_client, "/v1/archives/test_project",
        test_auth, "tests/data/blank_tar.tar"
    )
    if _request_accepted(response):
        response = background_job_runner(
            test_client, "upload", response, expect_success=False
        )
    assert response.status_code == 200
    assert response.json["errors"][0]["message"] == (
        "Blank tar archives are not supported."
    )
