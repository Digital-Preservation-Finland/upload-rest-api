"""Tests for ``upload_rest_api.app`` module"""
from __future__ import unicode_literals

import io
import json
import os
import shutil
import time

import pytest


def _contains_symlinks(fpath):
    """Check if fpath or any subdirectories contains symlinks

    :param fpath: Path to directory to check
    :returns: True if any symlinks are found else False
    """
    for dirpath, _, files in os.walk(fpath):
        for _file in files:
            if os.path.islink("%s/%s" % (dirpath, _file)):
                return True

    return False


def _set_user_quota(users, username, quota, used_quota):
    """Set quota and used quota of user username"""
    users.update_one({"_id": username}, {"$set": {"quota": quota}})
    users.update_one({"_id": username}, {"$set": {"used_quota": used_quota}})


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


def _wait_response(test_client, response, test_auth):
    status = "pending"
    polling_url = json.loads(response.data)["polling_url"]
    while status == "pending":
        time.sleep(0.1)
        response = test_client.get(polling_url, headers=test_auth)
        data = json.loads(response.data)
        status = data['status']
    return response, polling_url


def _request_accepted(response):
    """Returns True if request was accepted"""
    return response.status_code == 202


def test_index(app, test_auth, wrong_auth):
    """Test the application index page with correct
    and incorrect credentials.
    """
    test_client = app.test_client()

    response = test_client.get("/", headers=test_auth)
    assert response.status_code == 404

    response = test_client.get("/", headers=wrong_auth)
    assert response.status_code == 401


def test_upload(app, test_auth, mock_mongo):
    """Test uploading a plain text file"""
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    checksums = mock_mongo.upload.checksums

    response = _upload_file(
        test_client, "/v1/files/test.txt",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 200

    fpath = os.path.join(upload_path, "test_project/test.txt")
    assert os.path.isfile(fpath)
    assert open(fpath, "rb").read() == open("tests/data/test.txt", "rb").read()

    # Check that the uploaded files checksum was added to mongo
    checksum = checksums.find_one({"_id": fpath})["checksum"]
    assert checksum == "150b62e4e7d58c70503bd5fc8a26463c"

    # Test that trying to upload the file again returns 409 Conflict
    response = _upload_file(
        test_client, "/v1/files/test.txt",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 409


def test_upload_max_size(app, test_auth):
    """Test uploading file larger than the supported max file size"""
    # Set max upload size to 1 byte
    app.config["MAX_CONTENT_LENGTH"] = 1
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")

    response = _upload_file(
        test_client, "/v1/files/test.txt",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 413

    # Check that file was not saved on the server
    fpath = os.path.join(upload_path, "test/test.txt")
    assert not os.path.isfile(fpath)


def test_user_quota(app, test_auth, mock_mongo):
    """Test uploading files larger than allowed by user quota"""
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    users = mock_mongo.upload.users

    _set_user_quota(users, "test", 200, 0)
    response = _upload_file(
        test_client, "/v1/files/test.zip",
        test_auth, "tests/data/test.zip"
    )
    assert response.status_code == 413

    # Check that the file was not actually created
    assert not os.path.isdir(os.path.join(upload_path, "test_project"))


def test_used_quota(app, test_auth, mock_mongo, requests_mock):
    """Test that used quota is calculated correctly"""
    # Mock Metax
    requests_mock.get("https://metax-test.csc.fi/rest/v1/files?limit=10000&"
                      "project_identifier=test_project",
                      json={'next': None, 'results': []})

    test_client = app.test_client()
    users = mock_mongo.upload.users

    # Upload two 31B txt files
    _upload_file(
        test_client, "/v1/files/test1",
        test_auth, "tests/data/test.txt"
    )
    _upload_file(
        test_client, "/v1/files/test2",
        test_auth, "tests/data/test.txt"
    )
    used_quota = users.find_one({"_id": "test"})["used_quota"]
    assert used_quota == 62

    # Delete one of the files
    test_client.delete(
        "/v1/files/test1",
        headers=test_auth
    )
    used_quota = users.find_one({"_id": "test"})["used_quota"]
    assert used_quota == 31


def test_upload_outside(app, test_auth):
    """Test uploading outside the user's dir."""
    test_client = app.test_client()
    response = _upload_file(
        test_client, "/v1/files/../test.txt",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 404


@pytest.mark.parametrize("archive", [
    "tests/data/test.zip",
    "tests/data/test.tar.gz"
])
@pytest.mark.parametrize("dirpath", [True, False])
def test_upload_archive(archive, dirpath, app, test_auth, mock_mongo):
    """Test that uploaded archive is extracted. No files should be
    extracted outside the project directory.
    """
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    checksums = mock_mongo.upload.checksums
    url = "/v1/archives?dir=dataset" if dirpath else "/v1/archives"

    response = _upload_file(
        test_client, url, test_auth, archive
    )
    if _request_accepted(response):
        response, _ = _wait_response(test_client, response, test_auth)
    assert response.status_code == 200

    if not dirpath:
        fpath = os.path.join(upload_path, "test_project")
    else:
        fpath = os.path.join(upload_path, "test_project", "dataset")

    text_file = os.path.join(fpath, "test", "test.txt")
    archive_file = os.path.join(fpath, os.path.split(archive)[1])

    # test.txt is correctly extracted
    assert os.path.isfile(text_file)
    assert "test" in io.open(text_file, "rt").read()

    # archive file is removed
    assert not os.path.isfile(archive_file)

    # checksum is added to mongo
    assert checksums.count({}) == 1
    checksum = checksums.find_one({"_id": text_file})["checksum"]
    assert checksum == "150b62e4e7d58c70503bd5fc8a26463c"

    # Trying to upload same zip again should return 409 - Conflict
    response = _upload_file(
        test_client, url,
        test_auth, "tests/data/test.zip"
    )

    if dirpath:
        data = json.loads(response.data)
        assert response.status_code == 409
        assert data["error"] == "Directory 'dataset' already exists"

    else:
        if _request_accepted(response):
            response, _ = _wait_response(test_client, response, test_auth)

        data = json.loads(response.data)
        assert response.status_code == 200
        assert data["status"] == "error"
        assert data["message"] == "File 'test/test.txt' already exists"


@pytest.mark.parametrize("dirpath", [
    "../",
    "dataset/../../",
    "/dataset"
])
def test_upload_invalid_dir(dirpath, app, test_auth):
    """Test that trying to extract outside the project return 404.
    """
    test_client = app.test_client()
    response = _upload_file(
        test_client,
        "/v1/archives?dir=%s" % dirpath,
        test_auth,
        "tests/data/test.zip"
    )
    assert response.status_code == 404


def test_upload_archive_concurrent(app, test_auth, mock_mongo):
    """Test that uploaded archive is extracted. No files should be
    extracted outside the project directory.
    """
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    checksums = mock_mongo.upload.checksums

    response_1 = _upload_file(
        test_client, "/v1/archives", test_auth,
        "tests/data/test.zip"
    )
    response_2 = _upload_file(
        test_client, "/v1/archives", test_auth,
        "tests/data/test2.zip"
    )
    # poll with response's polling_url
    if _request_accepted(response_1):
        response_1, polling_url = _wait_response(test_client, response_1,
                                                 test_auth)
        data = json.loads(response_1.data)
        assert response_1.status_code == 200
        assert data["status"] == "done"
        assert data["message"] == "Archive uploaded and extracted"

        response_1 = test_client.delete(polling_url, headers=test_auth)
        assert response_1.status_code == 404
        data = json.loads(response_1.data)
        assert data["status"] == "Not found"

    # poll with response's polling_url
    if _request_accepted(response_2):
        response_2, polling_url = _wait_response(test_client, response_2,
                                                 test_auth)
        data = json.loads(response_2.data)
        assert response_2.status_code == 200
        assert data["status"] == "done"
        assert data["message"] == "Archive uploaded and extracted"

        response_2 = test_client.delete(polling_url, headers=test_auth)
        assert response_2.status_code == 404
        data = json.loads(response_2.data)
        assert data["status"] == "Not found"

    fpath = os.path.join(upload_path, "test_project")

    # test.txt files correctly extracted
    test_text_file = os.path.join(fpath, "test", "test.txt")
    test_2_text_file = os.path.join(fpath, "test2", "test.txt")
    assert os.path.isfile(test_text_file)
    assert "test" in io.open(test_text_file, "rt").read()
    assert os.path.isfile(test_2_text_file)
    assert "test" in io.open(test_2_text_file, "rt").read()

    # archive file is removed
    archive_file1 = os.path.join(fpath,
                                 os.path.split("tests/data/test.zip")[1])
    archive_file2 = os.path.join(fpath,
                                 os.path.split("tests/data/test2.zip")[1])
    assert not os.path.isfile(archive_file1)
    assert not os.path.isfile(archive_file2)

    # checksum is added to mongo
    assert checksums.count() == 2
    checksum = checksums.find_one({"_id": test_text_file})["checksum"]
    assert checksum == "150b62e4e7d58c70503bd5fc8a26463c"
    checksum = checksums.find_one({"_id": test_2_text_file})["checksum"]
    assert checksum == "150b62e4e7d58c70503bd5fc8a26463c"


@pytest.mark.parametrize("archive", [
    "tests/data/symlink.zip",
    "tests/data/symlink.tar.gz"
])
def test_upload_invalid_archive(archive, app, test_auth, mock_mongo):
    """Test that trying to upload a archive with symlinks returns error
    and doesn't create any files.
    """
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    checksums = mock_mongo.upload.checksums

    response = _upload_file(
        test_client, "/v1/archives", test_auth, archive
    )
    if _request_accepted(response):
        response, _ = _wait_response(test_client, response, test_auth)

    data = json.loads(response.data)
    assert response.status_code == 200
    assert data["message"] == "File 'test/link' has unsupported type: SYM"

    fpath = os.path.join(upload_path, "test_project")
    text_file = os.path.join(fpath, "test", "test.txt")
    archive_file = os.path.join(fpath, os.path.split(archive)[1])

    # test.txt is not extracted
    assert not os.path.isfile(text_file)

    # archive file is removed
    assert not os.path.isfile(archive_file)

    # no checksums are added to mongo
    assert checksums.count({}) == 0


def test_upload_file_as_archive(app, test_auth):
    """Test that trying to upload a file as an archive returns an error.
    """
    test_client = app.test_client()

    response = _upload_file(
        test_client, "/v1/archives", test_auth, "tests/data/test.txt"
    )
    if _request_accepted(response):
        response, _ = _wait_response(test_client, response, test_auth)

    data = json.loads(response.data)
    assert response.status_code == 400
    assert data["error"] == "Uploaded file is not a supported archive"


def test_get_file(app, test_auth, test2_auth, test3_auth, mock_mongo):
    """Test GET for single file"""
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")

    os.makedirs(os.path.join(upload_path, "test_project"))
    fpath = os.path.join(upload_path, "test_project/test.txt")
    shutil.copy("tests/data/test.txt", fpath)
    mock_mongo.upload.checksums.insert_one({
        "_id": fpath, "checksum": "150b62e4e7d58c70503bd5fc8a26463c"
    })

    # GET file that exists
    response = test_client.get(
        "/v1/files/test.txt",
        headers=test_auth
    )

    assert response.status_code == 200
    data = json.loads(response.data)

    assert data["file_path"] == "/test.txt"
    assert data["md5"] == "150b62e4e7d58c70503bd5fc8a26463c"
    assert data["metax_identifier"] == "None"

    # GET file with user test2, which is in the same project
    response = test_client.get(
        "/v1/files/test.txt",
        headers=test2_auth
    )
    assert response.status_code == 200

    # GET file with user test3, which is not in the same project
    response = test_client.get(
        "/v1/files/test.txt",
        headers=test3_auth
    )
    assert response.status_code == 404


def test_delete_file(app, test_auth, requests_mock, mock_mongo):
    """Test DELETE for single file"""
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
            }
        ]
    }
    # Mock Metax
    requests_mock.get("https://metax-test.csc.fi/rest/v1/files?limit=10000&"
                      "project_identifier=test_project",
                      json=response)

    requests_mock.post("https://metax-test.csc.fi/rest/v1/files/datasets",
                       json={})

    requests_mock.delete("https://metax-test.csc.fi/rest/v1/files/foo",
                         json='/test.txt')

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    fpath = os.path.join(upload_path, "test_project/test.txt")

    os.makedirs(os.path.join(upload_path, "test_project"))
    shutil.copy("tests/data/test.txt", fpath)
    mock_mongo.upload.checksums.insert_one({"_id": fpath, "checksum": "foo"})

    # DELETE file that exists
    response = test_client.delete(
        "/v1/files/test.txt",
        headers=test_auth
    )

    assert response.status_code == 200
    assert json.loads(response.data)["metax"] == "/test.txt"
    assert not os.path.isfile(fpath)
    assert mock_mongo.upload.checksums.count({}) == 0

    # DELETE file that does not exist
    response = test_client.delete(
        "/v1/files/test.txt",
        headers=test_auth
    )
    assert response.status_code == 404


def test_get_files(app, test_auth):
    """Test GET for the whole project and a single directory"""
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")

    os.makedirs(os.path.join(upload_path, "test_project/test"))
    shutil.copy(
        "tests/data/test.txt",
        os.path.join(upload_path, "test_project/test1.txt")
    )
    shutil.copy(
        "tests/data/test.txt",
        os.path.join(upload_path, "test_project/test/test2.txt")
    )

    # GET whole project
    response = test_client.get(
        "/v1/files",
        headers=test_auth
    )
    response_trailing_slash = test_client.get(
        "/v1/files/",
        headers=test_auth
    )

    assert response.data == response_trailing_slash.data
    assert response.status_code == 200
    data = json.loads(response.data)

    assert data["/"] == ["test1.txt"]
    assert data["/test"] == ["test2.txt"]

    # GET single directory
    response = test_client.get(
        "/v1/files/test/",
        headers=test_auth
    )

    assert response.status_code == 200
    data = json.loads(response.data)

    assert data["file_path"]["/test"] == ["test2.txt"]


def test_delete_files(app, test_auth, requests_mock, mock_mongo):
    """Test DELETE for the whole project and a single dir"""
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
    requests_mock.get("https://metax-test.csc.fi/rest/v1/files?limit=10000&"
                      "project_identifier=test_project",
                      json=response)

    requests_mock.post("https://metax-test.csc.fi/rest/v1/files/datasets",
                       json={})

    requests_mock.delete("https://metax-test.csc.fi/rest/v1/files",
                         json=['/test/test.txt'])

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    test_path_1 = os.path.join(upload_path, "test_project/test.txt")
    test_path_2 = os.path.join(upload_path, "test_project/test/test.txt")

    os.makedirs(os.path.join(upload_path, "test_project", "test/"))
    shutil.copy("tests/data/test.txt", test_path_1)
    shutil.copy("tests/data/test.txt", test_path_2)
    checksums = mock_mongo.upload.checksums
    checksums.insert_many([
        {"_id": test_path_1, "checksum": "foo"},
        {"_id": test_path_2, "checksum": "foo"},
    ])

    # DELETE single directory
    response = test_client.delete(
        "/v1/files/test",
        headers=test_auth
    )
    if _request_accepted(response):
        response, _ = _wait_response(test_client, response, test_auth)

    assert response.status_code == 200
    assert json.loads(response.data)["metax"] == ["/test/test.txt"]
    assert not os.path.exists(os.path.split(test_path_2)[0])
    assert checksums.count({}) == 1

    # DELETE the whole project
    requests_mock.delete("https://metax-test.csc.fi/rest/v1/files",
                         json=['/test.txt'])
    response = test_client.delete(
        "/v1/files",
        headers=test_auth
    )
    if _request_accepted(response):
        response, _ = _wait_response(test_client, response, test_auth)

    assert response.status_code == 200
    assert json.loads(response.data)["metax"] == ["/test.txt"]
    assert not os.path.exists(os.path.split(test_path_1)[0])
    assert checksums.count({}) == 0

    # DELETE project that does not exist
    response = test_client.delete(
        "/v1/files",
        headers=test_auth
    )
    assert response.status_code == 404


def test_delete_metadata(app, test_auth, requests_mock, mock_mongo):
    """Test DELETE metadata for a directory and a single dir"""
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
    requests_mock.get("https://metax-test.csc.fi/rest/v1/files?limit=10000&"
                      "project_identifier=test_project",
                      json=response)
    requests_mock.post("https://metax-test.csc.fi/rest/v1/files/datasets",
                       json=['dataset&preferred&identifier'])
    requests_mock.get("https://metax-test.csc.fi/rest/v1/datasets?"
                      "preferred_identifier=dataset%26preferred%26identifier",
                      json={"preservation_state": 75})
    adapter = requests_mock.delete("https://metax-test.csc.fi/rest/v1/files",
                                   json={"deleted_files_count": 1})
    requests_mock.delete("https://metax-test.csc.fi/rest/v1/files/foo",
                         json={})

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    test_path_1 = os.path.join(upload_path, "test_project/test.txt")
    test_path_2 = os.path.join(upload_path, "test_project/test/test.txt")

    os.makedirs(os.path.join(upload_path, "test_project", "test/"))
    shutil.copy("tests/data/test.txt", test_path_1)
    shutil.copy("tests/data/test.txt", test_path_2)
    checksums = mock_mongo.upload.checksums
    checksums.insert_many([
        {"_id": test_path_1, "checksum": "foo"},
        {"_id": test_path_2, "checksum": "foo"},
    ])

    # DELETE metadata for single directory
    response = test_client.delete(
        "/v1/metadata/test",
        headers=test_auth
    )
    if _request_accepted(response):
        response, _ = _wait_response(test_client, response, test_auth)

    assert response.status_code == 200
    assert json.loads(response.data)["file_path"] == "/test"
    assert json.loads(response.data)["metax"] == {"deleted_files_count": 1}
    assert adapter.last_request.json() == ['bar']

    # DELETE metadata for single file
    response = test_client.delete(
        "/v1/metadata/test.txt",
        headers=test_auth
    )
    if _request_accepted(response):
        response, _ = _wait_response(test_client, response, test_auth)

    assert response.status_code == 200
    assert json.loads(response.data)["file_path"] == "/test.txt"
    assert json.loads(response.data)["metax"] == {}


def test_delete_metadata_dataset_accepted(app, test_auth, requests_mock,
                                          mock_mongo):
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
    requests_mock.get("https://metax-test.csc.fi/rest/v1/files?limit=10000&"
                      "project_identifier=test_project",
                      json=response)

    requests_mock.post("https://metax-test.csc.fi/rest/v1/files/datasets",
                       json=['dataset_preferred_identifier'])
    requests_mock.get("https://metax-test.csc.fi/rest/v1/datasets?"
                      "preferred_identifier=dataset_preferred_identifier",
                      json={"preservation_state": 80})
    adapter = requests_mock.delete("https://metax-test.csc.fi/rest/v1/files",
                                   json={"deleted_files_count": 0})
    requests_mock.delete("https://metax-test.csc.fi/rest/v1/files/foo",
                         json={})

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    test_path_1 = os.path.join(upload_path, "test_project/test.txt")
    test_path_2 = os.path.join(upload_path, "test_project/test/test.txt")

    os.makedirs(os.path.join(upload_path, "test_project", "test/"))
    shutil.copy("tests/data/test.txt", test_path_1)
    shutil.copy("tests/data/test.txt", test_path_2)
    checksums = mock_mongo.upload.checksums
    checksums.insert_many([
        {"_id": test_path_1, "checksum": "foo"},
        {"_id": test_path_2, "checksum": "foo"},
    ])

    # DELETE metadata for single directory
    response = test_client.delete(
        "/v1/metadata/test",
        headers=test_auth
    )
    if _request_accepted(response):
        response, _ = _wait_response(test_client, response, test_auth)

    assert json.loads(response.data)["file_path"] == "/test"
    assert json.loads(response.data)["metax"] == {"deleted_files_count": 0}
    assert adapter.last_request is None

    # DELETE metadata for single file
    response = test_client.delete(
        "/v1/metadata/test.txt",
        headers=test_auth
    )
    if _request_accepted(response):
        response, _ = _wait_response(test_client, response, test_auth)

    response = json.loads(response.data)
    assert response["code"] == 400
    assert response["error"] == "Metadata is part of an accepted dataset"


def test_post_metadata(app, test_auth, requests_mock):
    """Test posting file metadata to Metax"""

    test_client = app.test_client()

    # Upload file to test instance
    _upload_file(
        test_client, "/v1/files/foo", test_auth, "tests/data/test.txt"
    )

    # Mock Metax HTTP response
    requests_mock.post("https://metax-test.csc.fi/rest/v1/files/",
                       json={"foo": "bar"})

    response = test_client.post("/v1/metadata/*", headers=test_auth)
    if _request_accepted(response):
        response, _ = _wait_response(test_client, response, test_auth)

    assert response.status_code == 200
    assert json.loads(response.data) == {
        "code": 200,
        "metax_response": {"foo": "bar"},
        "status": "done"
    }


def test_post_metadata_failure(app, test_auth, requests_mock):
    """Try to post file metadata to Metax when the metadata already exists. API
    should return HTTP response with status code 200, and the error message
    from Metax.
    """

    test_client = app.test_client()

    # Upload file to test instance
    _upload_file(
        test_client, "/v1/files/foo", test_auth, "tests/data/test.txt"
    )

    # Mock Metax HTTP response
    response_json = {
        "file_path": ["a file with path /foo already exists in project bar"],
        "identifier": ["a file with given identifier already exists"],
        "error_identifier": "2019-08-23T12:46:11-971d8a58"
    }
    requests_mock.post("https://metax-test.csc.fi/rest/v1/files/",
                       status_code=400,
                       json=response_json)

    response = test_client.post("/v1/metadata/foo", headers=test_auth)
    if _request_accepted(response):
        response, _ = _wait_response(test_client, response, test_auth)

    assert response.status_code == 200
    assert json.loads(response.data) == {
        "code": 400,
        "metax_response": response_json,
        "status": "error"
    }
