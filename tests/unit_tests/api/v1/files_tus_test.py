"""Tests for `upload_rest_api.api.v1.files_tus` module."""

import pathlib
from io import BytesIO

import pytest
from flask_tus_io.resource import encode_tus_meta


def _do_tus_upload(test_client, upload_metadata, data, auth):
    """
    Perform a tus upload
    """
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

    assert resp.status_code == 204  # NO CONTENT


@pytest.mark.usefixtures("project")
def test_upload(test_client, app, test_auth, test_mongo):
    """
    Test uploading a small file
    """
    upload_path = app.config.get("UPLOAD_PROJECTS_PATH")

    data = b"XyzzyXyzzy"
    upload_metadata = {
        "project_id": "test_project",
        "filename": "test.txt",
        "file_path": "test.txt",
    }

    _do_tus_upload(
        test_client=test_client,
        upload_metadata=upload_metadata,
        auth=test_auth,
        data=data
    )

    # Uploaded file was added to database
    checksums = list(test_mongo.upload.checksums.find())
    assert len(checksums) == 1
    assert checksums[0]["_id"].endswith("test.txt")
    assert checksums[0]["checksum"] == "a5d1741953bf0c12b7a097f58944e474"

    fpath = pathlib.Path(upload_path) / "test_project/test.txt"

    assert fpath.read_bytes() == b"XyzzyXyzzy"

    # Check that the file has 664 permissions. The group write permission
    # is required, otherwise siptools-research will crash later.
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


@pytest.mark.usefixtures("project")
def test_upload_create_metadata(
        test_client, test_auth, test_mongo, requests_mock,
        background_job_runner):
    """
    Test uploading a small file and create Metax metadata for it
    """
    data = b"XyzzyXyzzy"
    upload_metadata = {
        "project_id": "test_project",
        "filename": "test.txt",
        "file_path": "test.txt",
        "create_metadata": "true"
    }

    _do_tus_upload(
        test_client=test_client,
        upload_metadata=upload_metadata,
        auth=test_auth,
        data=data
    )

    # Mock Metax response
    requests_mock.post(
        "https://metax.localdomain/rest/v2/files/",
        json={"success": [], "failed": ["fail1", "fail2"]}
    )

    tasks = list(test_mongo.upload.tasks.find())
    assert len(tasks) == 1

    task_id = str(tasks[0]["_id"])

    response = background_job_runner(test_client, "metadata", task_id=task_id)

    assert response.status_code == 200
    assert response.json == {
        "message": "Metadata created: /test.txt",
        "status": "done"
    }


@pytest.mark.usefixtures("project")
def test_upload_deep_directory(test_client, test_auth, test_mongo, upload_tmpdir):
    """
    Test uploading a small file within a directory hierarchy, and ensure
    the directories are created as well
    """
    data = b"XyzzyXyzzy"

    upload_metadata = {
        "project_id": "test_project",
        "filename": "test.txt",
        "file_path": "foo/bar/test.txt",
    }

    _do_tus_upload(
        test_client=test_client,
        upload_metadata=upload_metadata,
        auth=test_auth,
        data=data
    )

    # Uploaded file was added to database
    checksums = list(test_mongo.upload.checksums.find())
    assert len(checksums) == 1
    assert checksums[0]["_id"].endswith("test.txt")
    assert checksums[0]["checksum"] == "a5d1741953bf0c12b7a097f58944e474"

    # Intermediary directories created, and file is inside it
    content = (
        upload_tmpdir / "projects" / "test_project"
        / "foo" / "bar" / "test.txt"
    ).read_text(encoding="utf-8")
    assert content == "XyzzyXyzzy"


def test_upload_exceed_quota(test_client, test_auth, database):
    """
    Upload one file and try uploading a second file which would exceed the
    quota
    """
    database.projects.set_quota("test_project", 15)

    upload_metadata = {
        "project_id": "test_project",
        "filename": "test.txt",
        "file_path": "test.txt",
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
                    "project_id": "test_project",
                    "filename": "test2.txt",
                    "file_path": "test2.txt"
                })
            },
            **test_auth
        }
    )

    assert resp.status_code == 413
    assert resp.json["error"] == "Remaining user quota too low"


def test_upload_parallel_upload_exceed_quota(test_client, test_auth, database):
    """
    Start enough parallel uploads to exceed the user quota
    """
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
                        "project_id": "test_project",
                        "filename": f"test{i}.txt",
                        "file_path": f"test{i}.txt",
                    })
                },
                **test_auth
            }
        )
        assert resp.status_code == 201

    # Initiate the third upload, which would result in exceeding the quota.
    # This will fail.
    resp = test_client.post(
        "/v1/files_tus",
        headers={
            **{
                "Tus-Resumable": "1.0.0",
                "Upload-Length": "97",
                "Upload-Metadata": encode_tus_meta({
                    "project_id": "test_project",
                    "filename": "test2.txt",
                    "file_path": "test2.txt"
                })
            },
            **test_auth
        }
    )

    assert resp.status_code == 413
    assert resp.json["error"] == "Remaining user quota too low"


@pytest.mark.usefixtures("database")
def test_upload_conflict(test_client, test_auth):
    """
    Try initiating two uploads with the same file path
    """
    upload_metadata = {
        "project_id": "test_project",
        "filename": "test.txt",
        "file_path": "test.txt"
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
