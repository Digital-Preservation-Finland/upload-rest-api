"""Tests for ``upload_rest_api.app`` module."""
import os
import shutil

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


def test_delete_metadata(
        app, test_auth, requests_mock, test_mongo, background_job_runner
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
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v2/files?limit=10000&"
                      "project_identifier=test_project",
                      json=response)
    requests_mock.post("https://metax.fd-test.csc.fi/rest/v2/files/datasets",
                       json=['dataset_identifier'])
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v2/datasets/"
                      "dataset_identifier",
                      json={"preservation_state": 75})
    adapter = requests_mock.delete(
        "https://metax.fd-test.csc.fi/rest/v2/files",
        json={"deleted_files_count": 1}
    )
    requests_mock.delete("https://metax.fd-test.csc.fi/rest/v2/files/foo",
                         json={})

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    test_path_1 = os.path.join(upload_path, "test_project/test.txt")
    test_path_2 = os.path.join(upload_path, "test_project/test/test.txt")

    os.makedirs(os.path.join(upload_path, "test_project", "test/"))
    shutil.copy("tests/data/test.txt", test_path_1)
    shutil.copy("tests/data/test.txt", test_path_2)
    checksums = test_mongo.upload.checksums
    checksums.insert_many([
        {"_id": test_path_1, "checksum": "foo"},
        {"_id": test_path_2, "checksum": "foo"},
    ])

    # DELETE metadata for single directory
    response = test_client.delete(
        "/v1/metadata/test_project/test",
        headers=test_auth
    )
    if _request_accepted(response):
        response = background_job_runner(test_client, "metadata", response)

    assert response.status_code == 200
    assert response.json["message"] == "1 files deleted"
    assert adapter.last_request.json() == ['bar']

    # DELETE metadata for single file
    response = test_client.delete(
        "/v1/metadata/test_project/test.txt",
        headers=test_auth
    )
    if _request_accepted(response):
        response = background_job_runner(test_client, "metadata", response)

    assert response.status_code == 200
    assert response.json["message"] == "1 files deleted"


def test_delete_metadata_dataset_accepted(
        app, test_auth, requests_mock, test_mongo, background_job_runner
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
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v2/files?limit=10000&"
                      "project_identifier=test_project",
                      json=response)

    requests_mock.post("https://metax.fd-test.csc.fi/rest/v2/files/datasets",
                       json=['dataset_identifier'])
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v2/datasets/"
                      "dataset_identifier",
                      json={"preservation_state": 80})
    adapter = requests_mock.delete(
        "https://metax.fd-test.csc.fi/rest/v2/files",
        json={"deleted_files_count": 0}
    )
    requests_mock.delete("https://metax.fd-test.csc.fi/rest/v2/files/foo",
                         json={})

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    test_path_1 = os.path.join(upload_path, "test_project/test.txt")
    test_path_2 = os.path.join(upload_path, "test_project/test/test.txt")

    os.makedirs(os.path.join(upload_path, "test_project", "test/"))
    shutil.copy("tests/data/test.txt", test_path_1)
    shutil.copy("tests/data/test.txt", test_path_2)
    checksums = test_mongo.upload.checksums
    checksums.insert_many([
        {"_id": test_path_1, "checksum": "foo"},
        {"_id": test_path_2, "checksum": "foo"},
    ])

    # DELETE metadata for single directory
    response = test_client.delete(
        "/v1/metadata/test_project/test",
        headers=test_auth
    )
    if _request_accepted(response):
        response = background_job_runner(test_client, "metadata", response)

    assert response.json["message"] == "0 files deleted"
    assert adapter.last_request is None

    # DELETE metadata for single file
    response = test_client.delete(
        "/v1/metadata/test_project/test.txt",
        headers=test_auth
    )
    if _request_accepted(response):
        response = background_job_runner(
            test_client, "metadata", response, expect_success=False
        )

    assert response.json["errors"][0]["message"] \
        == "Metadata is part of an accepted dataset"


def test_post_metadata(app, test_auth, requests_mock, background_job_runner):
    """Test posting file metadata to Metax."""
    test_client = app.test_client()

    # Upload file to test instance
    _upload_file(
        test_client,
        "/v1/files/test_project/foo", test_auth, "tests/data/test.txt"
    )

    # Mock Metax HTTP response
    requests_mock.post(
        "https://metax.fd-test.csc.fi/rest/v2/files/",
        json={"success": [], "failed": ["fail1", "fail2"]}
    )

    response = test_client.post(
        "/v1/metadata/test_project/*", headers=test_auth
    )
    if _request_accepted(response):
        response = background_job_runner(test_client, "metadata", response)

    assert response.status_code == 200
    assert response.json == {
        "message": "Metadata created: /",
        "status": "done"
    }


def test_post_metadata_missing_path(test_auth, test_client):
    """
    Test posting file metadata to Metax when the local file does not exist
    """
    response = test_client.post(
        "/v1/metadata/test_project/this/does/not/exist", headers=test_auth
    )
    assert response.status_code == 404
    assert response.json == {
        "code": 404,
        "error": "File not found"
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
        test_client, "/v1/files/test_project/foo", test_auth,
        "tests/data/test.txt"
    )

    # Mock Metax HTTP response
    requests_mock.post("https://metax.fd-test.csc.fi/rest/v2/files/",
                       status_code=400,
                       json=metax_response)

    response = test_client.post(
        "/v1/metadata/test_project/foo", headers=test_auth
    )
    if _request_accepted(response):
        response = background_job_runner(
            test_client, "metadata", response, expect_success=False
        )

    assert response.status_code == 200
    assert response.json == expected_response
