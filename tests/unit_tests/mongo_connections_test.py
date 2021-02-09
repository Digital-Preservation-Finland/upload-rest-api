"""Tests for ``upload_rest_api.app`` module."""
from __future__ import unicode_literals

import json
import time

import pymongo
import mock


def _upload_archive(client, auth):
    """Upload 1000_files.tar.gz archive.

    :returns: HTTP response
    """
    with open("tests/data/1000_files.tar.gz", "rb") as archive:
        response = client.post(
            "/v1/archives",
            input_stream=archive,
            headers=auth
        )
        assert response.status_code == 202

    return response


def _wait_response(client, response, auth, sleep):
    status = "pending"
    polling_url = json.loads(response.data)["polling_url"]
    while status == "pending":
        time.sleep(sleep)
        response = client.get(polling_url, headers=auth)
        data = json.loads(response.data)
        status = data["status"]

    assert response.status_code == 200
    assert status == "done"
    return response, polling_url


def test_upload_archive(app, test_auth, background_job_runner):
    """Test mongo connections for archive upload."""
    client = app.test_client()

    with mock.patch(
        "pymongo.MongoClient",
        return_value=pymongo.MongoClient()
    ) as connect:
        response = _upload_archive(client, test_auth)
        background_job_runner(client, "upload", response)
        assert connect.call_count < 10


def test_get_files(app, test_auth, background_job_runner):
    """Test mongo connections of a GET request to project root."""
    client = app.test_client()
    response = _upload_archive(client, test_auth)
    background_job_runner(client, "upload", response)

    with mock.patch(
        "pymongo.MongoClient",
        return_value=pymongo.MongoClient()
    ) as connect:
        # GET whole project
        client.get("/v1/files", headers=test_auth)
        assert connect.call_count == 2


def test_delete_files(app, test_auth, requests_mock, background_job_runner):
    """Test mongo connections of project deletion."""
    client = app.test_client()
    response = _upload_archive(client, test_auth)
    background_job_runner(client, "upload", response)

    # Mock Metax
    response = {
        "next": None,
        "results": [
            {
                "id": "foo",
                "identifier": "foo",
                "file_path": "test/%s.txt" % i,
                "file_storage": {
                    "identifier": "urn:nbn:fi:att:file-storage-pas"
                }
            } for i in range(1000)
        ]
    }
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v1/files?limit=10000&"
                      "project_identifier=test_project",
                      json=response)

    requests_mock.post("https://metax.fd-test.csc.fi/rest/v1/files/datasets",
                       json={})

    requests_mock.delete("https://metax.fd-test.csc.fi/rest/v1/files",
                         json=['test/%i.txt' % i for i in range(1000)])

    with mock.patch(
        "pymongo.MongoClient",
        return_value=pymongo.MongoClient()
    ) as connect:
        # DELETE the whole project
        response = client.delete(
            "/v1/files",
            headers=test_auth
        )
        background_job_runner(client, "files", response)
        assert connect.call_count < 10


def test_post_metadata(app, test_auth, requests_mock, background_job_runner):
    """Test posting file metadata to Metax."""
    client = app.test_client()
    _upload_archive(client, test_auth)

    # Mock Metax HTTP response
    requests_mock.post("https://metax.fd-test.csc.fi/rest/v1/files/",
                       json={"foo": "bar"})

    with mock.patch(
        "pymongo.MongoClient",
        return_value=pymongo.MongoClient()
    ) as connect:
        response = client.post("/v1/metadata/test/", headers=test_auth)
        background_job_runner(
            client, "metadata", response, expect_success=False
        )
        assert connect.call_count < 10


def test_delete_metadata(app, test_auth, requests_mock, background_job_runner):
    """Test mongo connections of metadata deletion."""
    client = app.test_client()
    response = _upload_archive(client, test_auth)
    background_job_runner(client, "upload", response)

    # Mock Metax
    response = {
        "next": None,
        "results": [
            {
                "id": "foo",
                "identifier": "foo",
                "file_path": "/test/%i.txt" % i,
                "file_storage": {
                    "identifier": "urn:nbn:fi:att:file-storage-pas"
                }
            } for i in range(1000)
        ]
    }
    requests_mock.get("https://metax.fd-test.csc.fi/rest/v1/files?limit=10000&"
                      "project_identifier=test_project",
                      json=response)

    requests_mock.post("https://metax.fd-test.csc.fi/rest/v1/files/datasets",
                       json=['dataset_identifier'])

    requests_mock.get("https://metax.fd-test.csc.fi/rest/v1/datasets/"
                      "dataset_identifier",
                      json={"preservation_state": 75})

    requests_mock.delete("https://metax.fd-test.csc.fi/rest/v1/files",
                         json={"deleted_files_count": 1000})

    requests_mock.delete("https://metax.fd-test.csc.fi/rest/v1/files/foo",
                         json={})

    with mock.patch(
        "pymongo.MongoClient",
        return_value=pymongo.MongoClient()
    ) as connect:
        # DELETE project metadata
        response = client.delete(
            "/v1/metadata/test/",
            headers=test_auth
        )
        background_job_runner(client, "metadata", response)
        assert connect.call_count < 10
