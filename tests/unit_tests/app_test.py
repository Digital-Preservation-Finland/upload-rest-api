"""Tests for ``upload_rest_api.app`` module"""
from __future__ import unicode_literals

import io
import json
import os
import shutil

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


def test_index(app, test_auth, wrong_auth):
    """Test the application index page with correct
    and incorrect credentials.
    """
    test_client = app.test_client()

    response = test_client.get("/", headers=test_auth)
    assert response.status_code == 404

    response = test_client.get("/", headers=wrong_auth)
    assert response.status_code == 401


def test_upload(app, test_auth, database_fx):
    """Test uploading a plain text file"""
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    checksums = database_fx.upload.checksums

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


def test_user_quota(app, test_auth, database_fx):
    """Test uploading files larger than allowed by user quota"""
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    users = database_fx.upload.users

    _set_user_quota(users, "test", 200, 0)
    response = _upload_file(
        test_client, "/v1/files/test.zip",
        test_auth, "tests/data/test.zip"
    )
    assert response.status_code == 413

    # Check that the file was not actually created
    assert not os.path.isdir(os.path.join(upload_path, "test_project"))


def test_used_quota(app, test_auth, database_fx, requests_mock):
    """Test that used quota is calculated correctly"""
    # Mock Metax
    requests_mock.get("https://metax-test.csc.fi/rest/v1/files/",
                      json={'next': None, 'results': []})

    test_client = app.test_client()
    users = database_fx.upload.users

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
def test_upload_archive(archive, app, test_auth, database_fx):
    """Test that uploaded arhive is extracted. No files should be
    extracted outside the project directory.
    """
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    checksums = database_fx.upload.checksums

    response = _upload_file(
        test_client, "/v1/files/archive", test_auth, archive
    )
    assert response.status_code == 200

    fpath = os.path.join(upload_path, "test_project")
    text_file = os.path.join(fpath, "test", "test.txt")
    archive_file = os.path.join(fpath, os.path.split(archive)[1])

    # test.txt is correctly extracted
    assert os.path.isfile(text_file)
    assert "test" in io.open(text_file, "rt").read()

    # archive file is removed
    assert not os.path.isfile(archive_file)

    # checksum is added to mongo
    assert checksums.find().count() == 1
    checksum = checksums.find_one({"_id": text_file})["checksum"]
    assert checksum == "150b62e4e7d58c70503bd5fc8a26463c"

    # Trying to upload same zip again should return 409 - Conflict
    response = _upload_file(
        test_client, "/v1/files/test.zip",
        test_auth, "tests/data/test.zip"
    )

    data = json.loads(response.data)
    assert response.status_code == 409
    assert data["error"] == "File 'test/test.txt' already exists"


@pytest.mark.parametrize("archive", [
    "tests/data/symlink.zip",
    "tests/data/symlink.tar.gz"
])
def test_upload_invalid_archive(archive, app, test_auth, database_fx):
    """Test that trying to upload a archive with symlinks return 413
    and doesn't create any files.
    """
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    checksums = database_fx.upload.checksums

    response = _upload_file(
        test_client, "/v1/files/archive", test_auth, archive
    )
    data = json.loads(response.data)
    assert response.status_code == 415
    assert data["error"] == "File 'test/link' has unsupported type: SYM"

    fpath = os.path.join(upload_path, "test_project")
    text_file = os.path.join(fpath, "test", "test.txt")
    archive_file = os.path.join(fpath, os.path.split(archive)[1])

    # test.txt is not extracted
    assert not os.path.isfile(text_file)

    # archive file is removed
    assert not os.path.isfile(archive_file)

    # no checksums are added to mongo
    assert checksums.find().count() == 0


def test_get_file(app, admin_auth, test_auth, test2_auth, database_fx):
    """Test GET for single file"""
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")

    os.makedirs(os.path.join(upload_path, "test_project"))
    fpath = os.path.join(upload_path, "test_project/test.txt")
    shutil.copy("tests/data/test.txt", fpath)
    database_fx.upload.checksums.insert_one({
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

    # GET file with user admin, which is not in the same project
    response = test_client.get(
        "/v1/files/test.txt",
        headers=admin_auth
    )
    assert response.status_code == 404


def test_delete_file(app, test_auth, requests_mock, database_fx):
    """Test DELETE for single file"""
    # Mock Metax
    requests_mock.get("https://metax-test.csc.fi/rest/v1/files/",
                      json={'next': None,
                            'results': [{'id': 'foo',
                                         'identifier': 'foo',
                                         'file_path': '/test.txt'}]})

    requests_mock.post("https://metax-test.csc.fi/rest/v1/files/datasets",
                       json={})

    requests_mock.delete("https://metax-test.csc.fi/rest/v1/files/foo",
                         json='/test.txt')

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    fpath = os.path.join(upload_path, "test_project/test.txt")

    os.makedirs(os.path.join(upload_path, "test_project"))
    shutil.copy("tests/data/test.txt", fpath)
    database_fx.upload.checksums.insert_one({"_id": fpath, "checksum": "foo"})

    # DELETE file that exists
    response = test_client.delete(
        "/v1/files/test.txt",
        headers=test_auth
    )

    assert response.status_code == 200
    assert json.loads(response.data)["metax"] == "/test.txt"
    assert not os.path.isfile(fpath)
    assert database_fx.upload.checksums.find().count() == 0

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


def test_delete_files(app, test_auth, requests_mock, database_fx):
    """Test DELETE for the whole project and a single dir"""
    # Mock Metax
    requests_mock.get("https://metax-test.csc.fi/rest/v1/files/",
                      json={
                          'next': None,
                          'results': [
                              {
                                  'id': 'foo',
                                  'identifier': 'foo',
                                  'file_path': '/test.txt'
                              },
                              {
                                  'id': 'bar',
                                  'identifier': 'bar',
                                  'file_path': '/test/test.txt'
                              }
                          ]
                      })

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
    checksums = database_fx.upload.checksums
    checksums.insert_many([
        {"_id": test_path_1, "checksum": "foo"},
        {"_id": test_path_2, "checksum": "foo"},
    ])

    # DELETE single directory
    response = test_client.delete(
        "/v1/files/test",
        headers=test_auth
    )

    assert response.status_code == 200
    assert json.loads(response.data)["metax"] == ["/test/test.txt"]
    assert not os.path.exists(os.path.split(test_path_2)[0])
    assert checksums.find().count() == 1

    # DELETE the whole project
    requests_mock.delete("https://metax-test.csc.fi/rest/v1/files",
                         json=['/test.txt'])
    response = test_client.delete(
        "/v1/files",
        headers=test_auth
    )

    assert response.status_code == 200
    assert json.loads(response.data)["metax"] == ["/test.txt"]
    assert not os.path.exists(os.path.split(test_path_1)[0])
    assert checksums.find().count() == 0

    # DELETE project that does not exist
    response = test_client.delete(
        "/v1/files",
        headers=test_auth
    )
    assert response.status_code == 404


def test_db_access_test_user(app, test_auth):
    """Test database access with some other user than admin"""
    test_client = app.test_client()

    response = test_client.get("/v1/users/user", headers=test_auth)
    assert response.status_code == 401

    response = test_client.post(
        "/v1/users/user/user_project", headers=test_auth
    )
    assert response.status_code == 401

    response = test_client.delete("/v1/users/user", headers=test_auth)
    assert response.status_code == 401


def test_get_user(app, admin_auth):
    """Test get_user() function"""
    test_client = app.test_client()

    # Existing user
    response = test_client.get("/v1/users/test", headers=admin_auth)
    data = json.loads(response.data)
    assert data["_id"] == "test"
    assert response.status_code == 200

    # User that does not exist
    response = test_client.get("/v1/users/user", headers=admin_auth)
    assert response.status_code == 404


def test_get_all_users(app, admin_auth, test_auth):
    """Test get_all_users() function"""
    test_client = app.test_client()

    response = test_client.get("/v1/users", headers=admin_auth)
    data = json.loads(response.data)
    assert data["users"] == ["admin", "test", "test2"]

    response = test_client.get("/v1/users", headers=test_auth)
    assert response.status_code == 401


def test_create_user(app, admin_auth, database_fx):
    """Test creating a new user"""
    test_client = app.test_client()

    # Create user that exists
    response = test_client.post(
        "/v1/users/test/test_project", headers=admin_auth
    )
    assert response.status_code == 409

    # Create user that does not exist
    response = test_client.post(
        "/v1/users/user/user_project", headers=admin_auth
    )
    data = json.loads(response.data)

    # Check response
    assert response.status_code == 200
    assert data["username"] == "user"
    assert data["project"] == "user_project"
    assert len(data["password"]) == 20

    # Check user from database
    users = database_fx.upload.users
    data = users.find_one({"_id": "user"})

    assert data is not None
    assert data["project"] == "user_project"
    assert len(data["digest"]) == 64
    assert len(data["salt"]) == 20
    assert data["quota"] == 5 * 1024**3
    assert data["used_quota"] == 0


def test_delete_user(app, admin_auth, database_fx):
    """Test deleting test user"""
    test_client = app.test_client()

    # Delete user that does not exist
    response = test_client.delete("/v1/users/test", headers=admin_auth)
    data = json.loads(response.data)

    assert response.status_code == 200
    assert data["username"] == "test"
    assert data["status"] == "deleted"

    # Check that user was deleted from the database
    users = database_fx.upload.users
    assert users.find_one({"_id": "test"}) is None


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

    response = test_client.post("/v1/metadata/foo", headers=test_auth)
    assert response.status_code == 200
    assert json.loads(response.data) == {
        "code": 200,
        "metax_response": {"foo": "bar"}
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
    assert response.status_code == 400
    assert json.loads(response.data) == {
        "code": 400,
        "metax_response": response_json
    }
