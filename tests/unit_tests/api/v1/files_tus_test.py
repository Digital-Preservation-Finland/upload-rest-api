"""Tests for `upload_rest_api.api.v1.files_tus` module."""

from io import BytesIO

import pytest

from flask_tus_io.resource import encode_tus_meta


@pytest.mark.usefixtures("project")
def test_upload(test_client, test_auth, test_mongo):
    """
    Test uploading a small file
    """
    data = b"XyzzyXyzzy"

    upload_metadata = {
        "project_id": "test_project",
        "filename": "test.txt",
        "file_path": "test.txt",
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

    assert resp.status_code == 201  # CREATED
    location = resp.location

    resp = test_client.head(
        location,
        headers={
            **{"Tus-Resumable": "1.0.0"},
            **test_auth
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
            **test_auth
        },
        input_stream=BytesIO(data)
    )

    assert resp.status_code == 204  # NO CONTENT

    # Uploaded file was added to database
    checksums = list(test_mongo.upload.checksums.find())
    assert len(checksums) == 1
    assert checksums[0]["_id"].endswith("test.txt")
    assert checksums[0]["checksum"] == "a5d1741953bf0c12b7a097f58944e474"

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
    location = resp.location

    resp = test_client.head(
        location,
        headers={
            **{"Tus-Resumable": "1.0.0"},
            **test_auth
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
            **test_auth
        },
        input_stream=BytesIO(data)
    )

    assert resp.status_code == 204

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
                        "filename": "test{}.txt".format(i),
                        "file_path": "test{}.txt".format(i)
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
