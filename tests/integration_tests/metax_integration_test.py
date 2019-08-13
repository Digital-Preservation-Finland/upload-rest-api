"""Integration tests using test-metax. URL endpoints that send requests to
Metax are tested. Tests make sure that the metadata is correctly posted and
deleted.

These tests require the METAX_URL, METAX_USER and METAX_PASSWORD to be defined
in /etc/upload_rest_api.conf.
"""
from __future__ import unicode_literals

import os
import getpass
import json
from runpy import run_path

import pytest
import pymongo

from upload_rest_api.gen_metadata import MetaxClient
import upload_rest_api.database as db
import upload_rest_api.cleanup as clean


URL = "https://metax-test.csc.fi"
USER = "tpas"

if os.path.isfile("/etc/upload_rest_api.conf"):
    PASSWORD = run_path("/etc/upload_rest_api.conf")["METAX_PASSWORD"]
else:
    PASSWORD = getpass.getpass(
        prompt="https://metax-test.csc.fi password for user tpas: "
    )

def _upload_file(client, url, auth, fpath):
    """Send POST request to given URL with file fpath

    :returns: HTTP response
    """
    with open(fpath, "rb") as test_file:
        response = client.post(
            url,
            input_stream=test_file,
            headers=auth
        )

    return response


@pytest.fixture(autouse=True)
def clean_metax():
    """DELETE all metadata from Metax that might be left from previous runs"""
    metax_client = MetaxClient(URL, USER, PASSWORD)
    files_dict = metax_client.get_files_dict("test_project")
    file_id_list = [value["id"] for value in files_dict.values()]
    metax_client.client.delete_files(file_id_list)


@pytest.mark.parametrize("dataset", [True, False])
def test_gen_metadata_root(app, dataset, test_auth, monkeypatch):
    """Test that calling /v1/metadata. produces
    correct metadata for all files of the project and
    metadata is removed when the file is removed.
    """
    if dataset:
        # Mock file_has_dataset to always return True
        monkeypatch.setattr(
            MetaxClient,
            "file_has_dataset",
            lambda a, b, c: True
        )

    app.config["METAX_PASSWORD"] = PASSWORD
    test_client = app.test_client()

    # Upload integration.zip, which is extracted by the server
    _upload_file(
        test_client, "/v1/files/integration.zip",
        test_auth, "tests/data/integration.zip"
    )

    # Generate and POST metadata for all the files in test_project
    response = test_client.post("/v1/metadata/*", headers=test_auth)

    assert response.status_code == 200
    data = json.loads(response.data)

    # All metadata POSTs succeeded
    assert not data["failed"]
    assert len(data["success"]) == 2

    # DELETE single file
    response = test_client.delete(
        "/v1/files/integration/test1/test1.txt",
        headers=test_auth
    )

    assert response.status_code == 200
    data = json.loads(response.data)

    if dataset:
        assert data["metax"].startswith("Metadata is part of a dataset")
    else:
        assert data["metax"]["deleted_files_count"] == 1

    # Test that test1.txt was removed from Metax but test2.txt is still there
    metax_client = MetaxClient(URL, USER, PASSWORD)
    files_dict = metax_client.get_files_dict("test_project")

    if dataset:
        assert len(files_dict) == 2
    else:
        assert len(files_dict) == 1

    assert "/integration/test2/test2.txt" in files_dict


@pytest.mark.parametrize("dataset", [True, False])
def test_gen_metadata_file(app, dataset, test_auth, monkeypatch):
    """Test that generating metadata for a single file works and the metadata
    is removed when project is deleted.
    """
    if dataset:
        # Mock file_has_dataset to always return True
        monkeypatch.setattr(
            MetaxClient,
            "file_has_dataset",
            lambda a, b, c: True
        )

    app.config["METAX_PASSWORD"] = PASSWORD
    test_client = app.test_client()

    # Upload integration.zip, which is extracted by the server
    _upload_file(
        test_client, "/v1/files/integration.zip",
        test_auth, "tests/data/integration.zip"
    )

    # Generate and POST metadata for file test1.txt in test_project
    response = test_client.post(
        "/v1/metadata/integration/test1/test1.txt",
        headers=test_auth
    )

    assert response.status_code == 200
    data = json.loads(response.data)

    # All metadata POSTs succeeded
    assert not data["failed"]
    assert len(data["success"]) == 1

    # DELETE whole project
    response = test_client.delete("/v1/files", headers=test_auth)

    assert response.status_code == 200
    data = json.loads(response.data)

    if dataset:
        assert data["metax"]["deleted_files_count"] == 0
    else:
        assert data["metax"]["deleted_files_count"] == 1

    # Test that no test_project files are found in Metax
    metax_client = MetaxClient(URL, USER, PASSWORD)
    files_dict = metax_client.get_files_dict("test_project")

    if dataset:
        assert len(files_dict) == 1
    else:
        assert not files_dict


@pytest.mark.parametrize("dataset", [True, False])
def test_disk_cleanup(app, dataset, test_auth, monkeypatch):
    """Test that cleanup script removes file metadata from Metax if it is
    not associated with any dataset.
    """
    # Mock configuration parsing
    def _mock_conf(fpath):
        if not os.path.isfile(fpath):
            fpath = "include/etc/upload_rest_api.conf"

        conf = run_path(fpath)
        conf["METAX_PASSWORD"] = PASSWORD
        conf["UPLOAD_PATH"] = app.config.get("UPLOAD_PATH")
        conf["CLEANUP_TIMELIM"] = -1

        return conf

    monkeypatch.setattr(clean, "parse_conf", _mock_conf)

    if dataset:
        # Mock file_has_dataset to always return True
        monkeypatch.setattr(
            MetaxClient,
            "file_has_dataset",
            lambda a, b, c: True
        )

    app.config["METAX_PASSWORD"] = PASSWORD
    test_client = app.test_client()

    # Upload integration.zip, which is extracted by the server
    _upload_file(
        test_client, "/v1/files/integration.zip",
        test_auth, "tests/data/integration.zip"
    )

    # Generate and POST metadata for all the files in test_project
    test_client.post("/v1/metadata/*", headers=test_auth)

    # Cleanup all files
    clean.clean_disk()

    # Test that no test_project files are found in Metax
    metax_client = MetaxClient(URL, USER, PASSWORD)
    files_dict = metax_client.get_files_dict("test_project")

    if dataset:
        assert len(files_dict) == 2
    else:
        assert not files_dict


def test_mongo_cleanup(app, test_auth, monkeypatch):
    """Test that cleaning files from mongo deletes all files that
    haven't been posted to Metax.
    """
    app.config["METAX_PASSWORD"] = PASSWORD
    test_client = app.test_client()

    # Mock FilesCol mongo connection
    def _mock_init(self):
        host = app.config.get("MONGO_HOST")
        port = app.config.get("MONGO_PORT")
        self.files = pymongo.MongoClient(host, port).upload.files

    monkeypatch.setattr(db.FilesCol, "__init__", _mock_init)

    # Mock configuration parsing
    def _mock_conf(fpath):
        if not os.path.isfile(fpath):
            fpath = "include/etc/upload_rest_api.conf"

        conf = run_path(fpath)
        conf["METAX_PASSWORD"] = PASSWORD
        conf["UPLOAD_PATH"] = app.config.get("UPLOAD_PATH")
        conf["CLEANUP_TIMELIM"] = -1

        return conf

    monkeypatch.setattr(clean, "parse_conf", _mock_conf)

    files_col = db.FilesCol()

    # ----- Inserting fake identifiers to Mongo and cleaning them
    files_col.insert([
        {"_id": "pid:urn:1", "file_path": "1"},
        {"_id": "pid:urn:2", "file_path": "2"}
    ])
    assert len(files_col.get_all_ids()) == 2

    clean.clean_mongo()
    assert not files_col.get_all_ids()

    # Upload integration.zip, which is extracted by the server
    _upload_file(
        test_client, "/v1/files/integration.zip",
        test_auth, "tests/data/integration.zip"
    )

    # Generate and POST metadata for all the files in test_project
    test_client.post("/v1/metadata/*", headers=test_auth)

    # Check that generated identifiers were added to Mongo
    assert len(files_col.get_all_ids()) == 2

    # Check that generated file_paths resolve to actual files
    for file_doc in files_col.files.find():
        file_path = file_doc["file_path"]
        assert os.path.isfile(file_path)

    # Try to clean file documents that still exist in Metax
    clean.clean_mongo()

    assert len(files_col.get_all_ids()) == 2
