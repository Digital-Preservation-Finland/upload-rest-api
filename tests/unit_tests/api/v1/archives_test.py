"""Tests for ``upload_rest_api.app`` module."""
import os
import pathlib

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
        archive, app, test_auth, test_mongo, background_job_runner
):
    """Test uploading archive.

    Test that:

    * API response contains correct message
    * Files are extracted to correct location
    * Archive file is removed after extraction
    * Checksums of files in archive are added to database

    :param archive: path to test archive
    :param app: Flask app
    :param test_auth: authentication headers
    :param test_mongo: Mongoclient
    :param background_job_runner: RQ job mocker
    """
    test_client = app.test_client()
    upload_path = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH"))
    checksums = test_mongo.upload.checksums

    url = "/v1/archives/test_project"
    response = _upload_file(test_client, url, test_auth, archive)
    assert response.status_code == 202
    assert response.json['file_path'] == '/'
    assert response.json["message"] == "Uploading archive"
    assert response.json["polling_url"].startswith(
        'http://localhost/v1/tasks/'
    )
    assert response.json["status"] == "pending"
    assert response.headers['Location'] == response.json["polling_url"]

    # archive file is saved to temporary path
    upload_tmp_path = pathlib.Path(app.config.get("UPLOAD_TMP_PATH"))
    tmp_files = [path for path in upload_tmp_path.iterdir() if path.is_file()]
    assert len(tmp_files) == 1

    # Complete the task and check task status
    response = background_job_runner(test_client, "upload", response)
    assert response.status_code == 200
    assert response.json['status'] == 'done'

    # test.txt is correctly extracted
    fpath = upload_path / "test_project"
    text_file = fpath / "test" / "test.txt"
    assert text_file.is_file()
    assert "test" in text_file.read_text()

    # archive file is removed
    assert not tmp_files[0].is_file()

    # checksum is added to mongo
    assert checksums.count({}) == 1
    checksum = checksums.find_one({"_id": str(text_file)})["checksum"]
    assert checksum == "150b62e4e7d58c70503bd5fc8a26463c"


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
        dirpath, app, test_auth, background_job_runner
):
    """Test that archive is extracted to path given as parameter.

    :param dirpath: Directory path where archive is extracted
    :param app: Flask app
    :param test_auth: authentication headers
    :param background_job_runner: RQ job mocker
    """
    test_client = app.test_client()

    url = "/v1/archives/test_project?dir={}".format(dirpath)
    response \
        = _upload_file(test_client, url, test_auth, 'tests/data/test.tar.gz')
    assert response.status_code == 202
    assert response.json['file_path'] == '/{}'.format(dirpath.strip('/'))

    # Complete the task
    response = background_job_runner(test_client, "upload", response)

    # test.txt is correctly extracted
    text_file = (pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH"))
                 / 'test_project' / dirpath.lstrip('/')
                 / "test" / "test.txt")
    assert text_file.is_file()
    assert "test" in text_file.read_text()


@pytest.mark.parametrize(
    ["archive", "url"],
    [
        ("tests/data/test.tar.gz", "/v1/archives/test_project?dir=dataset"),
        ("tests/data/file1.tar", "/v1/archives/test_project?dir=dataset")
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
    assert response.status_code == 409
    assert response.json["error"] == "Directory 'dataset' already exists"


@pytest.mark.parametrize(
    ["archive", "url"],
    [
        ("tests/data/test.tar.gz", "/v1/archives/test_project")
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

    assert response.status_code == 200
    assert response.json["status"] == "error"
    assert response.json["errors"][0]["message"] \
        == "File 'test/test.txt' already exists"


@pytest.mark.parametrize(
    ('checksum', 'expected_status_code', 'expected_response'),
    [
        # The actual md5sum of tests/data/test.tar.gz
        (
            '78b925c44b7425e90686fb104ee0569b',
            202,
            {
                'file_path': '/test_directory',
                'message': 'Uploading archive',
                'status': 'pending',
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
def test_archive_integrity_validation(
        app, test_auth, mock_redis, checksum, expected_status_code,
        expected_response):
    """Test integrity validation of uploaded archive.

    Upload archive with checksum provided in HTTP request header.

    :param app: Flask app
    :param test_auth: authentication headers
    :param cheksum: checksum included in HTTP headers
    :param expected_status_code: expected status of response from API
    :param expected_response: expected JSON response from API
    """
    # Post archive
    test_client = app.test_client()
    with open('tests/data/test.tar.gz', "rb") as test_file:
        response = test_client.post(
            '/v1/archives/test_project',
            query_string={'dir': 'test_directory', 'md5': checksum},
            input_stream=test_file,
            headers=test_auth
        )

    # Check response
    assert response.status_code == expected_status_code
    for key in expected_response:
        assert response.json[key] == expected_response[key]

    # Target directory should not have created yet
    assert not os.path.exists(
        os.path.join(app.config.get('UPLOAD_PROJECTS_PATH'),
                     'test_project',
                     'test_directory')
    )

    # Remove the dangling lock for the background job
    if response.status_code == 202:
        mock_redis.flushall()


@pytest.mark.parametrize(
    ["archive1", "url1", "archive2", "url2"],
    [
        (
            "tests/data/dir1_file1.tar",
            "/v1/archives/test_project",
            "tests/data/dir1_file2.tar",
            "/v1/archives/test_project"
        ),
        # TODO: For some reason this test case fails. See issue
        # TPASPKT-722
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
    assert response.status_code == 200
    assert response.json["status"] == "done"
    assert response.json["message"] == "Archive uploaded and extracted"


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
        "/v1/archives/test_project?dir=%s" % dirpath,
        test_auth,
        "tests/data/test.zip"
    )
    assert response.status_code == 404
    assert response.json['error'] == "Page not found"


def test_upload_archive_multiple_archives(
        app, test_auth, test_mongo, background_job_runner
):
    """Test that uploaded archive is extracted.

    No files should be extracted outside the project directory.
    """
    test_client = app.test_client()
    upload_path = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH"))
    checksums = test_mongo.upload.checksums

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
        assert response_1.json["message"] == "Archive uploaded and extracted"

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
        assert response_2.json["message"] == "Archive uploaded and extracted"

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

    # checksum is added to mongo
    assert checksums.count() == 2
    checksum = checksums.find_one({"_id": str(test_text_file)})["checksum"]
    assert checksum == "150b62e4e7d58c70503bd5fc8a26463c"
    checksum = checksums.find_one({"_id": str(test_2_text_file)})["checksum"]
    assert checksum == "150b62e4e7d58c70503bd5fc8a26463c"


@pytest.mark.parametrize("archive", [
    "tests/data/symlink.zip",
    "tests/data/symlink.tar.gz"
])
def test_upload_invalid_archive(
        archive, app, test_auth, test_mongo, background_job_runner):
    """Test that trying to upload a archive with symlinks returns error
    and doesn't create any files.
    """
    test_client = app.test_client()
    upload_path = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH"))
    checksums = test_mongo.upload.checksums

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

    # no checksums are added to mongo
    assert checksums.count({}) == 0


def test_upload_file_as_archive(app, test_auth, background_job_runner):
    """Test that trying to upload a file as an archive returns an
    error.
    """
    test_client = app.test_client()

    response = _upload_file(
        test_client, "/v1/archives/test_project", test_auth,
        "tests/data/test.txt"
    )
    if _request_accepted(response):
        response = background_job_runner(
            test_client, "upload", response, expect_success=False
        )

    assert response.status_code == 400
    assert response.json["error"] == "Uploaded file is not a supported archive"


def test_upload_large_archive(app, test_auth):
    """Test uploading too large archive."""
    app.config["MAX_CONTENT_LENGTH"] = 1

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
