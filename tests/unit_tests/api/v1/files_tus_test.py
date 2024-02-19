"""Tests for `upload_rest_api.api.v1.files_tus` module."""

import pathlib
from io import BytesIO

import pytest
from flask_tus_io.resource import encode_tus_meta

from upload_rest_api.jobs.utils import get_job_queue
from upload_rest_api.models.project import ProjectEntry


def _do_tus_upload(
        test_client, upload_metadata, data, auth,
        # 204 NO CONTENT by default
        expected_status=204):
    """Perform a tus upload."""
    upload_length = len(data)

    resp = test_client.post(
        "/v1/files_tus",
        headers={
            **{
                "Tus-Resumable": "1.0.0",
                "Upload-Length": str(upload_length),
                "Upload-Metadata": encode_tus_meta(upload_metadata)
            },
            **auth
        }
    )

    assert resp.status_code == 201  # CREATED
    location = resp.location

    resp = test_client.head(
        location,
        headers={
            **{"Tus-Resumable": "1.0.0"},
            **auth
        }
    )

    resp = test_client.patch(
        location,
        content_type="application/offset+octet-stream",
        headers={
            **{
                "Content-Type": "application/offset+octet-stream",
                "Tus-Resumable": "1.0.0",
                "Upload-Offset": "0"
            },
            **auth
        },
        input_stream=BytesIO(data)
    )

    assert resp.status_code == expected_status

    # Return the response and the upload identifier
    return resp, location.split("/")[-1]


@pytest.mark.usefixtures("project")
@pytest.mark.parametrize(
    "name", ("test.txt", "tämäontesti.txt", "tämä on testi.txt")
)
def test_upload_file(app, test_auth, test_mongo, name, requests_mock):
    """Test uploading a small file.

    :param app: Flask app
    :param test_auth: Authentication headers
    :param test_mongo: Mongo client
    :param name: Name of uploaded file
    :param requests_mock: HTTP request mocker
    """
    test_client = app.test_client()

    # Mock metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

    # Upload file
    file_content = b"XyzzyXyzzy"
    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "filename": name,
        "upload_path": name,
    }
    _do_tus_upload(
        test_client=test_client,
        upload_metadata=upload_metadata,
        auth=test_auth,
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
def test_upload_file_checksum(test_client, test_auth, requests_mock):
    """Test uploading a file with a checksum.

    Ensure that the checksum is checked.

    :param test_client: Flask client
    :param test_auth: Authentication headers
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

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
    _do_tus_upload(
        test_client=test_client,
        upload_metadata=upload_metadata,
        auth=test_auth,
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

    response, _ = _do_tus_upload(
        test_client=test_client,
        upload_metadata=upload_metadata,
        auth=test_auth,
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
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

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
def test_upload_file_checksum_incorrect_syntax(test_client, test_auth):
    """Test uploading a file with a checksum using incorrect syntax."""
    data = b"XyzzyXyzzy"

    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "filename": "test.txt",
        "upload_path": "test.txt",
        "checksum": "f96ab43c95b09f803870f8cbf83e"
    }

    resp, _ = _do_tus_upload(
        test_client=test_client,
        upload_metadata=upload_metadata,
        auth=test_auth,
        data=data,
        expected_status=400
    )

    assert resp.json["error"] \
        == "Checksum does not follow '<alg>:<checksum>' syntax"


@pytest.mark.usefixtures("project", "mock_redis")
def test_upload_file_checksum_unknown_algorithm(test_client, test_auth):
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

    resp, _ = _do_tus_upload(
        test_client=test_client,
        upload_metadata=upload_metadata,
        auth=test_auth,
        data=data,
        expected_status=400
    )

    assert resp.json["error"] == "Unrecognized hash algorithm 'blake3'"


@pytest.mark.usefixtures("project")
@pytest.mark.parametrize(
    "name", ("test.txt", "tämäontesti.txt", "tämä on testi.txt")
)
def test_upload_file_deep_directory(
    test_client, test_auth, test_mongo, name, mock_config, requests_mock
):
    """Test uploading a small file within a directory hierarchy.

    Ensure that the directories are created as well.
    """
    # Mock Metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

    data = b"XyzzyXyzzy"

    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "filename": name,
        "upload_path": f"foo/bar/{name}",
    }

    _do_tus_upload(
        test_client=test_client,
        upload_metadata=upload_metadata,
        auth=test_auth,
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


def test_upload_file_exceed_quota(test_client, test_auth, requests_mock):
    """Test exceeding quota.

    Upload one file and try uploading a second file which would exceed
    the quota.
    """
    # Mock Metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

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

    _do_tus_upload(
        test_client=test_client,
        upload_metadata=upload_metadata,
        auth=test_auth,
        data=data
    )

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
def test_upload_large_file(test_client, test_auth, test_mongo, requests_mock):
    """
    Test uploading a large file.

    Finalizing the upload (checksum calculation and storing the file)
    is done in a background task for large files.
    """
    # Mock metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

    # Upload file
    file_content = b"A" * (128 * 1024 * 1024)
    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "checksum": "sha2:eadaaf6bbacea8cabc6b4c3def3d1e4c76577249c01c0065bd9dd78a1c5a47b5",
        "filename": "test.txt",
        "upload_path": "test.txt",
    }
    _do_tus_upload(
        test_client=test_client,
        upload_metadata=upload_metadata,
        auth=test_auth,
        data=file_content
    )

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
        test_client, test_auth, test_mongo, requests_mock):
    """
    Test uploading a large file with the wrong checksum
    """
    # Mock metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

    # Upload file
    file_content = b"A" * (128 * 1024 * 1024)
    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "checksum": "sha2:4d67c5cb3a0e17aaf578f9c8fee20d1f55d608acf064602349b9516046bee671",
        "filename": "test.txt",
        "upload_path": "test.txt",
    }
    _, upload_id = _do_tus_upload(
        test_client=test_client,
        upload_metadata=upload_metadata,
        auth=test_auth,
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
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})
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
        test_client, test_auth, requests_mock):
    """Test parallel uploads to /v1/files_tus API and /v1/files API.

    If all quota has been alloceted for TUS uploads, it should not be
    possible to upload files using /v1/files API.
    """
    # Mock metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

    project = ProjectEntry.objects.get(id="test_project")
    project.quota = 2000
    project.save()

    # Allocate almost all quota for a TUS upload
    tus_upload_data = b'0' * 1999
    tus_upload = test_client.post(
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

    # Try to upload a small file using /v1/files API.
    upload = test_client.post(
            '/v1/files/test_project/foo',
            data=b'foobar',
            headers=test_auth
    )
    assert upload.status_code == 413
    assert upload.json['error'] == 'Quota exceeded'

    # The TUS upload can be continued
    tus_upload = test_client.patch(
        tus_upload.location,
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
    assert tus_upload.status_code == 204


def test_upload_file_conflict(test_client, test_auth, requests_mock):
    """Try initiating two uploads with the same file path."""
    # Mock metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

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


def test_mixed_parallel_upload_conflict(test_client, test_auth, requests_mock):
    """Test parallel uploads to /v1/files_tus API and /v1/files API.

    If a TUS upload has been started, uploading a file to conflicting
    path using /v1/files API should not be possible.
    """
    # Mock metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

    # Start a tus upload to '/test.txt'
    data = b'foo'
    tus_upload = test_client.post(
        "/v1/files_tus",
        headers={
            **{
                "Tus-Resumable": "1.0.0",
                "Upload-Length": str(len(data)),
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

    # Try to upload a file to same path using /v1/files API
    upload = test_client.post(
            '/v1/files/test_project/test.txt',
            data=data,
            headers=test_auth
    )
    assert upload.status_code == 409
    assert upload.json['error'] \
        == 'The file/directory is currently locked by another task'

    # The TUS upload can be continued
    tus_upload = test_client.patch(
        tus_upload.location,
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
    assert tus_upload.status_code == 204


def test_upload_archive(test_client, test_auth, mock_config, requests_mock):
    """Test uploading an archive.

    Ensure that the archive is extracted successfully.

    :param test_client: Flask client
    :param test_auth: Authorization headers
    :param mock_config: Configuration
    :param requests_mock: HTTP request mocker
    """
    # Mock Metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

    upload_metadata = {
        "type": "archive",
        "project_id": "test_project",
        "filename": "test.zip",
        "upload_path": "extract_dir",
    }

    test_data = pathlib.Path("tests/data/test.zip").read_bytes()

    _do_tus_upload(
        test_client=test_client,
        upload_metadata=upload_metadata,
        auth=test_auth,
        data=test_data
    )

    # Run the upload background job
    queue = get_job_queue('upload')
    queue.run_job(queue.jobs[0])

    base_path = pathlib.Path(mock_config["UPLOAD_BASE_PATH"])
    assert (
        base_path / "projects" / "test_project" / "extract_dir"
        / "test" / "test.txt"
    ).read_text(encoding="utf-8").startswith("test file for REST file upload")


@pytest.mark.usefixtures("project")
def test_compute_checksum_once(app, test_auth, requests_mock,
                               mock_get_file_checksum):
    """Test that file checksum is computed only once.

    :param app: Flask app
    :param test_auth: Authentication headers
    :param requests_mock: HTTP request mocker
    :param mock_get_file_checksum: get_file_checksum mock
    """
    test_client = app.test_client()

    # Mock metax
    metax_files_api = requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

    # Upload file
    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "filename": 'foo',
        "upload_path": 'foo',
    }
    _do_tus_upload(
        test_client=test_client,
        upload_metadata=upload_metadata,
        auth=test_auth,
        data=b"asdf"
    )

    # The correct checksum should be posted to Metax, but the checksum
    # should be computed only during the TUS upload. Checksum should NOT
    # be computed again when metadata is generated.
    assert metax_files_api.last_request.json()[0]['checksum']['value'] \
        == '912ec803b2ce49e4a541068d495ab570'
    mock_get_file_checksum.assert_not_called()
