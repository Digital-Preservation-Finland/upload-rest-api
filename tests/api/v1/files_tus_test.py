"""Tests for `upload_rest_api.api.v1.files_tus` module."""

import logging
import pathlib
import urllib
import re
from io import BytesIO

import pytest
from flask_tus_io.resource import encode_tus_meta

from upload_rest_api.jobs.utils import get_job_queue
from upload_rest_api.models.project import ProjectEntry
from upload_rest_api.models.file_entry import FileEntry


@pytest.mark.usefixtures("project")
@pytest.mark.parametrize(
    "name", ("test.txt", "tämäontesti.txt", "tämä on testi.txt")
)
def test_upload_file(
        app, tus_upload, test_auth, test_mongo, name, caplog, requests_mock):
    """Test uploading a small file.

    :param app: Flask app
    :param test_auth: Authentication headers
    :param test_mongo: Mongo client
    :param name: Name of uploaded file
    :param requests_mock: HTTP request mocker
    """
    caplog.set_level(logging.INFO)

    test_client = app.test_client()

    # Mock metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get(f'/v3/files?pathname=%2F{urllib.parse.quote(name, safe="")}&csc_project=test_project',
                      json={'results': [], 'next': None})
    # Upload file
    file_content = b"XyzzyXyzzy"
    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "filename": name,
        "upload_path": name,
    }
    tus_upload(
        upload_metadata=upload_metadata,
        data=file_content
    )

    # Uploaded file was added to database
    files = list(test_mongo.upload.files.find())
    assert len(files) == 1
    assert files[0]["_id"].endswith(name)
    assert files[0]["checksum"] == "a5d1741953bf0c12b7a097f58944e474"

    # Uploaded file is written to expected path
    fpath = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH")) \
        / "test_project" / name
    assert fpath.read_bytes() == file_content

    # Check that the file has 664 permissions. The group write
    # permission is required, otherwise siptools-research will crash
    # later.
    assert oct(fpath.stat().st_mode)[5:8] == "664"

    used_quota = test_mongo.upload.projects.find_one(
        {"_id": "test_project"}
    )["used_quota"]
    assert used_quota == 10

    # Completed upload is logged with upload type and file name
    assert any(
        record for record in caplog.records
        if record.levelname == "INFO"
        and "Stored UploadType.FILE" in record.message
        and name in record.message
    )

    # Another upload to the same path can't be initiated
    resp = test_client.post(
        "/v1/files_tus",
        headers={
            **{
                "Tus-Resumable": "1.0.0",
                "Upload-Length": "10",
                "Upload-Metadata": encode_tus_meta(upload_metadata)
            },
            **test_auth
        }
    )

    assert resp.status_code == 409
    assert resp.json == {"code": 409,
                         "error": f"File '/{name}' already exists",
                         "files": [f"/{name}"]}



@pytest.mark.usefixtures("project", "mock_redis")
def test_upload_file_checksum(tus_upload, requests_mock):
    """Test uploading a file with a checksum.

    Ensure that the checksum is checked.

    :param test_client: Flask client
    :param test_auth: Authentication headers
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files?pathname=%2Ftest.txt&csc_project=test_project',
                      json={'results': [], 'next': None})

    data = b"XyzzyXyzzy"
    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "filename": "test.txt",
        "upload_path": "test.txt",
        "checksum": (
            "sha256:f96ab43c95b09f803870f8cbf83e"
            "b0f30e3a5a2e29741c3aedb2cf696a155bbc"
        )
    }

    # First upload with correct checksum succeeds
    tus_upload(
        upload_metadata=upload_metadata,
        data=data,
        expected_status=204
    )

    # Try again with incorrect checksum
    upload_metadata["upload_path"] = "test2.txt"
    upload_metadata["filename"] = "test2.txt"
    upload_metadata["checksum"] = (
        "sha256:f96ab43c95b09f803870f8cbf83eb0"
        "f30e3a5a2e29741c3aedb2cf696a155bbd"
    )

    response, _ = tus_upload(
        upload_metadata=upload_metadata,
        data=data,
        expected_status=400
    )

    assert response.json["error"] == "Upload checksum mismatch"


@pytest.mark.usefixtures("project")
def test_upload_file_checksum_iterative(
        app, test_client, test_auth, test_mongo, requests_mock
):
    """
    Test uploading a file in multiple parts and ensure the checksum
    is calculated correctly
    """
    # Mock Metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files?pathname=%2Ftest.txt&csc_project=test_project',
                      json={'results': [], 'next': None})
    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "filename": "test.txt",
        "upload_path": "test.txt",
        "checksum": (
            "sha256:d134cfd960e025a14b65c8ab3ff61"
            "2d40957c9af0025ded4810d8f2d312455a8"
        )
    }

    resp = test_client.post(
        "/v1/files_tus",
        headers={
            **{
                "Tus-Resumable": "1.0.0",
                "Upload-Length": "25",
                "Upload-Metadata": encode_tus_meta(upload_metadata)
            },
            **test_auth
        }
    )

    assert resp.status_code == 201  # CREATED
    location = resp.location

    resp = test_client.head(
        location,
        headers={
            **{"Tus-Resumable": "1.0.0"},
            **test_auth
        }
    )

    # Upload 'XyzzyXyzzyXyzzyXyzzyXyzzy' in 5 chunks
    for i in range(0, 5):
        resp = test_client.patch(
            location,
            content_type="application/offset+octet-stream",
            headers={
                **{
                    "Content-Type": "application/offset+octet-stream",
                    "Tus-Resumable": "1.0.0",
                    "Upload-Offset": str(i*5)
                },
                **test_auth
            },
            input_stream=BytesIO(b"Xyzzy")
        )

        assert resp.status_code == 204

    # Check that correct checksum was added to database
    upload_path = pathlib.Path(app.config.get("UPLOAD_PROJECTS_PATH"))
    assert test_mongo.upload.files.find_one(
        {
            "_id": str(upload_path / "test_project" / "test.txt"),
            "checksum": "71680afeb1ac710d2cc230b96c9cc894"
        }
    )


@pytest.mark.usefixtures("project", "mock_redis")
def test_upload_file_checksum_incorrect_syntax(tus_upload):
    """Test uploading a file with a checksum using incorrect syntax."""
    data = b"XyzzyXyzzy"

    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "filename": "test.txt",
        "upload_path": "test.txt",
        "checksum": "f96ab43c95b09f803870f8cbf83e"
    }

    resp, _ = tus_upload (
        upload_metadata=upload_metadata,
        data=data,
        expected_status=400
    )

    assert resp.json["error"] \
        == "Checksum does not follow '<alg>:<checksum>' syntax"


@pytest.mark.usefixtures("project", "mock_redis")
def test_upload_file_checksum_unknown_algorithm(tus_upload):
    """
    Test uploading a file with a checksum using an unknown algorithm,
    and ensure it is rejected.
    """
    data = b"XyzzyXyzzy"

    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "filename": "test.txt",
        "upload_path": "test.txt",
        "checksum": (
            "blake3:6a7145de535a6c211debe731e215a"
            "4e422f220910bfada7cee2e6c0c3170a55a"
        )
    }

    resp, _ = tus_upload(
        upload_metadata=upload_metadata,
        data=data,
        expected_status=400
    )

    assert resp.json["error"] == "Unrecognized hash algorithm 'blake3'"


def test_upload_conflicting_directory(
        test_client, tus_upload_file, requests_mock, mock_redis):
    """Test uploading file to path that is a directory."""
    # Mock metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files?pathname=%2Ffoo%2Fbar&csc_project=test_project',
                      json={'results': [], 'next': None})

    # First upload file to "/foo/bar", so that directory "/foo" is
    # created.
    tus_upload_file(
        project_id="test_project",
        path="foo/bar"
    )

    # Then, try to upload file to "/foo". It should fail because
    # directory "/foo" exists
    response, _ = tus_upload_file(
        project_id="test_project",
        path="foo",
        expected_status=409
    )
    assert response.json['error'] == "Directory '/foo' already exists"


def test_upload_outside_project_directory(tus_upload_file):
    """Test attempting an upload outside project directory; this should fail.

    :param url: URL of request
    :param data: content of Request
    """
    response, _ = tus_upload_file(
        project_id="test_project",
        path="../test.txt",
        expected_status=400
    )
    assert response.status_code == 400
    assert response.json['error'] == 'Invalid path'


@pytest.mark.usefixtures("project")
@pytest.mark.parametrize(
    "name", ("test.txt", "tämäontesti.txt", "tämä on testi.txt")
)
def test_upload_file_deep_directory(
    tus_upload, test_mongo, name, mock_config, requests_mock
):
    """Test uploading a small file within a directory hierarchy.

    Ensure that the directories are created as well.
    """
    # Mock Metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get(f'/v3/files?pathname=%2F{urllib.parse.quote(f"foo/bar/{name}", safe="")}&csc_project=test_project',
                      json={'results': [], 'next': None})
    data = b"XyzzyXyzzy"

    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "filename": name,
        "upload_path": f"foo/bar/{name}",
    }

    tus_upload(
        upload_metadata=upload_metadata,
        data=data
    )

    # Uploaded file was added to database
    files = list(test_mongo.upload.files.find())
    assert len(files) == 1
    assert files[0]["_id"].endswith(name)
    assert files[0]["checksum"] == "a5d1741953bf0c12b7a097f58944e474"

    # Intermediary directories created, and file is inside it
    base_path = pathlib.Path(mock_config["UPLOAD_BASE_PATH"])
    assert (
        base_path / "projects" / "test_project" / "foo" / "bar" / name
    ).read_bytes() == data


def test_upload_file_exceed_quota(
        test_client, test_auth, tus_upload, requests_mock):
    """Test exceeding quota.

    Upload one file and try uploading a second file which would exceed
    the quota.
    """
    # Mock Metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files?pathname=%2Ftest.txt&csc_project=test_project',
                      json={'results': [], 'next': None})
    project = ProjectEntry.objects.get(id="test_project")
    project.quota = 15
    project.save()

    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "filename": "test.txt",
        "upload_path": "test.txt",
    }
    data = b"XyzzyXyzzy"

    tus_upload(upload_metadata=upload_metadata, data=data)

    # 10 bytes of quota used, the next upload shoud fail
    resp = test_client.post(
        "/v1/files_tus",
        headers={
            **{
                "Tus-Resumable": "1.0.0",
                "Upload-Length": "10",
                "Upload-Metadata": encode_tus_meta({
                    "type": "file",
                    "project_id": "test_project",
                    "filename": "test2.txt",
                    "upload_path": "test2.txt"
                })
            },
            **test_auth
        }
    )

    assert resp.status_code == 413
    assert resp.json["error"] == "Quota exceeded"


@pytest.mark.usefixtures("project")
def test_upload_large_file(tus_upload, test_mongo, requests_mock):
    """
    Test uploading a large file.

    Finalizing the upload (checksum calculation and storing the file)
    is done in a background task for large files.
    """
    # Mock metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files?pathname=%2Ftest.txt&csc_project=test_project',
                      json={'results': [], 'next': None})
    # Upload file
    file_content = b"A" * (128 * 1024 * 1024)
    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "checksum": "sha2:eadaaf6bbacea8cabc6b4c3def3d1e4c76577249c01c0065bd9dd78a1c5a47b5",
        "filename": "test.txt",
        "upload_path": "test.txt",
    }
    tus_upload(upload_metadata=upload_metadata, data=file_content)

    # Uploaded file was not added to database yet
    files = list(test_mongo.upload.files.find())
    assert not files

    # Checksum calculation and finalization happens in a background task due
    # to the large size of the upload; run it
    queue = get_job_queue('upload')
    queue.run_job(queue.jobs[0])

    files = list(test_mongo.upload.files.find())
    assert len(files) == 1
    assert files[0]["_id"].endswith("/test.txt")
    assert files[0]["checksum"] == "2ab4abd9d93f6e2b90675d17effe1d62"


@pytest.mark.usefixtures("project")
def test_upload_large_file_wrong_checksum(
        test_client, test_auth, tus_upload, test_mongo, requests_mock):
    """
    Test uploading a large file with the wrong checksum
    """
    # Mock metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files?pathname=%2Ftest.txt&csc_project=test_project',
                      json={'results': [], 'next': None})
    # Upload file
    file_content = b"A" * (128 * 1024 * 1024)
    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "checksum": "sha2:4d67c5cb3a0e17aaf578f9c8fee20d1f55d608acf064602349b9516046bee671",
        "filename": "test.txt",
        "upload_path": "test.txt",
    }
    _, upload_id = tus_upload(
        upload_metadata=upload_metadata,
        data=file_content
    )

    # Uploaded file was not added to database yet
    files = list(test_mongo.upload.files.find())
    assert not files

    # Checksum calculation and finalization happens in a background task;
    # run it
    queue = get_job_queue('upload')
    queue.run_job(queue.jobs[0])

    files = list(test_mongo.upload.files.find())
    assert len(files) == 0

    response = test_client.get(
        f"/v1/tasks/{upload_id}",
        headers=test_auth
    )
    result = response.json

    # Upload task failed
    assert len(result["errors"]) == 1
    assert result["errors"][0] == {
        "files": None, "message": "Upload checksum mismatch"
    }


def test_upload_file_parallel_upload_exceed_quota(
        test_client, test_auth, requests_mock):
    """Start enough parallel uploads to exceed the user quota."""
    # Mock metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get(re.compile('/v3/files\?pathname=%2Ftest[0-2].txt&csc_project=test_project.*'),
                      json={'results': [], 'next': None})

    ProjectEntry.objects.filter(id="test_project").update_one(set__quota=2999)

    data = b'0' * 1000
    upload_length = len(data)

    # User has a quota of exactly 2999 bytes. Initiate three uploads,
    # the last of which will exceed this quota.
    responses = {}
    for i in range(0, 3):
        # Initiate upload, but don't finish it
        responses[i] = test_client.post(
            "/v1/files_tus",
            headers={
                **{
                    "Tus-Resumable": "1.0.0",
                    "Upload-Length": str(upload_length),
                    "Upload-Metadata": encode_tus_meta({
                        "type": "file",
                        "project_id": "test_project",
                        "filename": f"test{i}.txt",
                        "upload_path": f"test{i}.txt",
                    })
                },
                **test_auth
            }
        )

    # The first two uploads should be OK, but the third should fail.
    for i in range(0, 2):
        assert responses[i].status_code == 201
    assert responses[2].status_code == 413
    assert responses[2].json["error"] == "Quota exceeded"

    # The first two uploads can be continued
    for i in range(0, 2):
        response = test_client.patch(
            responses[i].location,
            content_type="application/offset+octet-stream",
            headers={
                **{
                    "Content-Type": "application/offset+octet-stream",
                    "Tus-Resumable": "1.0.0",
                    "Upload-Offset": "0"
                },
                **test_auth
            },
            input_stream=BytesIO(data)
        )
        assert response.status_code == 204


def test_mixed_parallel_upload_exceed_quota(
        test_client, test_auth, tus_upload, requests_mock):
    """Test parallel uploads to /v1/files_tus API.

    If all quota has been alloceted for TUS uploads, it should not be
    possible to upload files using /v1/files API.
    """
    # Mock metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files?pathname=%2Ftest.txt&csc_project=test_project',
                      json={'results': [], 'next': None})
    project = ProjectEntry.objects.get(id="test_project")
    project.quota = 2000
    project.save()

    # Allocate almost all quota for a TUS upload
    tus_upload_data = b'0' * 1999
    upload_resp = test_client.post(
        "/v1/files_tus",
        headers={
            **{
                "Tus-Resumable": "1.0.0",
                "Upload-Length": str(len(tus_upload_data)),
                "Upload-Metadata": encode_tus_meta({
                    "type": "file",
                    "project_id": "test_project",
                    "filename": "test.txt",
                    "upload_path": "test.txt",
                })
            },
            **test_auth
        }
    )
    # Try to upload a different file
    upload_resp_2, _ = tus_upload(
        upload_metadata={
            "type": "file",
            "project_id": "test_project",
            "filename": "test2.txt",
            "upload_path": "test2.txt"
        },
        data=b'foobar',
        expected_status=413  # REQUEST ENTITY TOO LARGE
    )
    assert upload_resp_2.json['error'] == 'Quota exceeded'

    # The TUS upload can be continued
    upload_resp = test_client.patch(
        upload_resp.location,
        content_type="application/offset+octet-stream",
        headers={
            **{
                "Content-Type": "application/offset+octet-stream",
                "Tus-Resumable": "1.0.0",
                "Upload-Offset": "0"
            },
            **test_auth
        },
        input_stream=BytesIO(tus_upload_data)
    )
    assert upload_resp.status_code == 204


def test_upload_file_conflict(
        test_client, test_auth, tus_upload_file, requests_mock):
    """Try initiating two uploads with the same file path."""
    # Mock metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files?pathname=%2Ftest.txt&csc_project=test_project',
                      json={'results': [], 'next': None})
    data = b'foo'
    upload_length = len(data)

    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "filename": "test.txt",
        "upload_path": "test.txt"
    }

    resp1 = test_client.post(
        "/v1/files_tus",
        headers={
            **{
                "Tus-Resumable": "1.0.0",
                "Upload-Length": str(upload_length),
                "Upload-Metadata": encode_tus_meta(upload_metadata)
            },
            **test_auth
        }
    )
    assert resp1.status_code == 201

    # Another upload with the same path can't be initiated
    resp2 = test_client.post(
        "/v1/files_tus",
        headers={
            **{
                "Tus-Resumable": "1.0.0",
                "Upload-Length": "10",
                "Upload-Metadata": encode_tus_meta(upload_metadata)
            },
            **test_auth
        }
    )
    assert resp2.status_code == 409
    assert resp2.json["error"] \
        == "The file/directory is currently locked by another task"

    # The first upload can be continued
    response = test_client.patch(
        resp1.location,
        content_type="application/offset+octet-stream",
        headers={
            **{
                "Content-Type": "application/offset+octet-stream",
                "Tus-Resumable": "1.0.0",
                "Upload-Offset": "0"
            },
            **test_auth
        },
        input_stream=BytesIO(data)
    )
    assert response.status_code == 204


def test_upload_oversized_file(
        tus_upload, test_client, mock_config, background_job_runner):
    """
    Test uploading an oversized file which
    exceeds the size limit for a single upload
    """
    mock_config["MAX_CONTENT_LENGTH"] = 1

    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "filename": "test",
        "upload_path": "foo"
    }
    resp, _ = tus_upload(
        upload_metadata=upload_metadata,
        data=b"five",
        expected_status=413  # REQUEST ENTITY TOO LARGE
    )

    assert resp.json["error"] == "Max single file size exceeded"


def test_upload_archive(
        tus_upload_archive, mock_config, requests_mock):
    """Test uploading an archive.

    Ensure that the archive is extracted successfully.

    :param mock_config: Configuration
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files?pathname=%2Fextract_dir&csc_project=test_project',
                      json={'results': [], 'next': None})

    tus_upload_archive(project_id="test_project", path="extract_dir")

    # Uploaded ZIP is stored in a temporary location
    assert len(list(pathlib.Path(mock_config["UPLOAD_TMP_PATH"]).iterdir())) \
        == 1

    # Run the upload background job
    queue = get_job_queue('upload')
    queue.run_job(queue.jobs[0])

    # File was created
    base_path = pathlib.Path(mock_config["UPLOAD_BASE_PATH"])
    assert (
        base_path / "projects" / "test_project" / "extract_dir"
        / "test" / "test.txt"
    ).read_text(encoding="utf-8").startswith("test file for REST file upload")

    # Temporary directory is empty
    assert not list(pathlib.Path(mock_config["UPLOAD_TMP_PATH"]).iterdir())

    # File documents are added
    assert FileEntry.objects.count() == 1
    file = FileEntry.objects.first()
    assert file.checksum == "150b62e4e7d58c70503bd5fc8a26463c"
    assert file.path == str(
        pathlib.Path(mock_config["UPLOAD_PROJECTS_PATH"])
        / "test_project/extract_dir/test/test.txt"
    )


def test_upload_multiple_archives_to_root(
        test_client, mock_config, tus_upload_archive, background_job_runner,
        requests_mock):
    """
    Test uploading multiple archives to project in order. Uploads
    should succeed since archive contents don't conflict.
    """
    # Mock metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files', json={'next': None, 'results': []})

    # Upload first archive
    _, upload_id = tus_upload_archive(
        project_id="test_project", src_path="tests/data/test.zip", path="/"
    )
    background_job_runner(test_client, "upload", task_id=upload_id)

    # Upload second archive
    _, upload_id = tus_upload_archive(
        project_id="test_project", src_path="tests/data/test2.zip", path="/"
    )
    background_job_runner(test_client, "upload", task_id=upload_id)

    project_root = \
        pathlib.Path(mock_config["UPLOAD_PROJECTS_PATH"]) / "test_project"

    # Both files exist and have correct content
    assert "test" in (project_root / "test/test.txt").read_text()
    assert "test" in (project_root / "test2/test.txt").read_text()

    assert FileEntry.objects.count() == 2
    assert FileEntry.objects.filter(
        path=str(project_root / "test/test.txt")
    ).count() == 1
    assert FileEntry.objects.filter(
        path=str(project_root / "test2/test.txt")
    ).count() == 1


@pytest.mark.parametrize(
    "dir_path",
    [
        "directory",
        "directory/subdirectory",
        "/directory",
        "///directory",
        "directory/"
    ]
)
def test_upload_archive_to_destination(
        test_client, mock_config, tus_upload_archive,
        background_job_runner, requests_mock, dir_path
):
    """Test that archive is extracted to path given as parameter.
    """
    # Mock metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files', json={'next': None, 'results': []})

    _, upload_id = tus_upload_archive(project_id="test_project", path=dir_path)

    # Complete the task
    response = background_job_runner(test_client, "upload", task_id=upload_id)
    assert response.status_code == 200
    assert response.json['message'] == \
        f'archive uploaded to /{dir_path.strip("/")}'

    # test.txt is correctly extracted
    text_file = (
        pathlib.Path(mock_config["UPLOAD_PROJECTS_PATH"])
        / 'test_project' / dir_path.lstrip('/')
        / "test" / "test.txt"
    )
    assert text_file.is_file()
    assert "test" in text_file.read_text()


@pytest.mark.parametrize(
    "archive",
    [
        "tests/data/symlink.zip",
        "tests/data/symlink.tar.gz"
    ]
)
def test_upload_invalid_archive(
        tus_upload, test_client, test_mongo, background_job_runner, archive):
    """Test uploading an invalid archive with symlinks"""
    upload_metadata = {
        "type": "archive",
        "project_id": "test_project",
        "filename": pathlib.Path(archive).name,
        "upload_path": "foo"
    }
    resp, upload_id = tus_upload(
        upload_metadata=upload_metadata,
        data=pathlib.Path(archive).read_bytes()
    )

    resp = background_job_runner(
        test_client, "upload", task_id=upload_id,
        expect_success=False
    )

    errors = resp.json["errors"]
    assert errors[0]["message"] == "File 'test/link' has unsupported type: SYM"

    assert FileEntry.objects.count() == 0  # No files added


def test_upload_archive_already_exists(
    test_client, test_auth, tus_upload_file, tus_upload_archive,
    requests_mock
):
    """Test uploading archive to path that is a file.
    """
    # Mock metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files', json={'next': None, 'results': []})

    # Upload a file to a path that will cause a conflict
    tus_upload_file(project_id="test_project", path="foo")

    # Try to upload an archive to same path
    response, _ = tus_upload_archive(
        project_id="test_project", path="foo", expected_status=409  # CONFLICT
    )
    assert response.json['error'] == "File '/foo' already exists"
    assert response.json['files'] == ['/foo']


def test_upload_empty_archive(
        test_client, tus_upload_archive, requests_mock, background_job_runner):
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files', json={'next': None, 'results': []})

    _, upload_id = tus_upload_archive(
        project_id="test_project", src_path="tests/data/blank_tar.tar",
        path="test"
    )

    response = background_job_runner(
        test_client, "upload", task_id=upload_id, expect_success=False
    )

    errors = response.json["errors"]

    assert errors[0]["message"] == "Blank tar archives are not supported."


def test_upload_no_rights(tus_upload_file):
    """
    Test uploading a file to a project that the user does not have
    permissions for
    """
    response, _ = tus_upload_file(
        project_id="test_project2", path="test",
        expected_status=403  # FORBIDDEN
    )

    assert "You don't have the permission" in response.json["error"]


def test_upload_archive_exceeding_quota(
        tus_upload, test_client, background_job_runner):
    """
    Test uploading an archive that would exceed the quota after extraction
    """
    # The only file in `2kb_test.tar.gz` is 2 KB, while the archive is only
    # 131 bytes; make quota 200 bytes to ensure the quota is exceeded
    # only during extraction.
    project = ProjectEntry.objects.get(id="test_project")
    project.quota = 200
    project.save()

    upload_metadata = {
        "type": "archive",
        "project_id": "test_project",
        "filename": "test.tar.gz",
        "upload_path": "foo"
    }
    resp, upload_id = tus_upload(
        upload_metadata=upload_metadata,
        data=pathlib.Path("tests/data/2kb_test.tar.gz").read_bytes()
    )

    resp = background_job_runner(
        test_client, "upload", task_id=upload_id, expect_success=False
    )

    errors = resp.json["errors"]

    assert errors[0]["message"] == "Quota exceeded"


@pytest.mark.usefixtures("project")
def test_compute_checksum_once(app, test_auth, tus_upload, requests_mock,
                               mock_get_file_checksum):
    """Test that file checksum is computed only once.

    :param app: Flask app
    :param test_auth: Authentication headers
    :param requests_mock: HTTP request mocker
    :param mock_get_file_checksum: get_file_checksum mock
    """
    test_client = app.test_client()

    # Mock metax
    metax_files_api = requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files?pathname=%2Ffoo&csc_project=test_project',
                      json={'results': [], 'next': None})
    # Upload file
    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "filename": 'foo',
        "upload_path": 'foo',
    }
    tus_upload(
        upload_metadata=upload_metadata,
        data=b"asdf"
    )

    # The correct checksum should be posted to Metax, but the checksum
    # should be computed only during the TUS upload. Checksum should NOT
    # be computed again when metadata is generated.
    assert metax_files_api.last_request.json()[0]['checksum'] \
        == 'md5:912ec803b2ce49e4a541068d495ab570'
    mock_get_file_checksum.assert_not_called()
