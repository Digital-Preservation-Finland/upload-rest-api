"""Tests for `upload_rest_api.api.v1.files_tus` module."""

import pathlib
from io import BytesIO

import pytest
from flask_tus_io.resource import encode_tus_meta

from upload_rest_api.jobs.utils import get_job_queue


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

    return resp


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
    assert resp.json == {"code": 409, "error": "File already exists"}


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
    response = _do_tus_upload(
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

    response = _do_tus_upload(
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

    resp = _do_tus_upload(
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

    resp = _do_tus_upload(
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


def test_upload_file_exceed_quota(test_client, test_auth, database,
                                  requests_mock):
    """Test exceeding quota.

    Upload one file and try uploading a second file which would exceed
    the quota.
    """
    # Mock Metax
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': [], 'next': None})

    database.projects.set_quota("test_project", 15)

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
    assert resp.json["error"] == "Remaining user quota too low"


def test_upload_file_parallel_upload_exceed_quota(
        test_client, test_auth, database):
    """Start enough parallel uploads to exceed the user quota."""
    database.projects.set_quota("test_project", 4096)

    # User has a quota of exactly 4096 bytes. Initiate three uploads,
    # the last of which will exceed this quota.
    for i in range(0, 2):
        # Initiate upload, but don't finish it
        resp = test_client.post(
            "/v1/files_tus",
            headers={
                **{
                    "Tus-Resumable": "1.0.0",
                    "Upload-Length": "2000",
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
        assert resp.status_code == 201

    # Initiate the third upload, which would result in exceeding the
    # quota. This will fail.
    resp = test_client.post(
        "/v1/files_tus",
        headers={
            **{
                "Tus-Resumable": "1.0.0",
                "Upload-Length": "97",
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
    assert resp.json["error"] == "Remaining user quota too low"


@pytest.mark.usefixtures("database")
def test_upload_file_conflict(test_client, test_auth):
    """Try initiating two uploads with the same file path."""
    upload_metadata = {
        "type": "file",
        "project_id": "test_project",
        "filename": "test.txt",
        "upload_path": "test.txt"
    }

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
    assert resp.status_code == 201

    # Another upload with the same path can't be initiated
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
    assert resp.json["error"] == "File already exists"


@pytest.mark.usefixtures("database")
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
