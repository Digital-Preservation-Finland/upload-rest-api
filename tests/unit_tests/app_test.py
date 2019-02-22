"""Tests for ``upload_rest_api.app`` module"""

import os
import shutil
import json

import upload_rest_api.gen_metadata as md
from tests.mockup.metax import MockMetax


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


def test_upload(app, test_auth):
    """Test uploading a plain text file"""
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")

    response = _upload_file(
        test_client, "/files/v1/test.txt",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 200

    fpath = os.path.join(upload_path, "test_project/test.txt")
    assert os.path.isfile(fpath)
    assert open(fpath, "rb").read() == open("tests/data/test.txt", "rb").read()

    # Test that trying to upload the file again returns 409 Conflict
    response = _upload_file(
        test_client, "/files/v1/test.txt",
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
        test_client, "/files/v1/test.txt",
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

    _set_user_quota(users, "test", 400, 0)
    response = _upload_file(
        test_client, "/files/v1/test",
        test_auth, "tests/data/test.zip"
    )
    assert response.status_code == 413

    # Check that the file was not actually created
    assert not os.path.isdir(os.path.join(upload_path, "test_project"))


def test_used_quota(app, test_auth, database_fx, monkeypatch):
    """Test that used quota is calculated correctly"""
    # Mock Metax
    monkeypatch.setattr(md, "MetaxClient", lambda: MockMetax())

    test_client = app.test_client()
    users = database_fx.upload.users

    # Upload two 31B txt files
    _upload_file(
        test_client, "/files/v1/test1",
        test_auth, "tests/data/test.txt"
    )
    _upload_file(
        test_client, "/files/v1/test2",
        test_auth, "tests/data/test.txt"
    )
    used_quota = users.find_one({"_id": "test"})["used_quota"]
    assert used_quota == 62

    # Delete one of the files
    test_client.delete(
        "/files/v1/test1",
        headers=test_auth
    )
    used_quota = users.find_one({"_id": "test"})["used_quota"]
    assert used_quota == 31


def test_upload_outside(app, test_auth):
    """Test uploading outside the user's dir."""
    test_client = app.test_client()
    response = _upload_file(
        test_client, "/files/v1/../test.txt",
        test_auth, "tests/data/test.txt"
    )

    assert response.status_code == 404


def test_upload_zip(app, test_auth):
    """Test that uploaded zip files are extracted. No files should be
    extracted outside the project directory.
    """
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")

    response = _upload_file(
        test_client, "/files/v1/test.zip",
        test_auth, "tests/data/test.zip"
    )
    assert response.status_code == 200

    fpath = os.path.join(upload_path, "test_project")
    text_file = os.path.join(fpath, "test", "test.txt")
    zip_file = os.path.join(fpath, "test.zip")

    # test.txt is correctly extracted
    assert os.path.isfile(text_file)
    assert "test" in open(text_file).read()

    # zip file is removed
    assert not os.path.isfile(zip_file)

    # no symlinks are created
    assert not _contains_symlinks(fpath)


def test_get_file(app, admin_auth, test_auth, test2_auth):
    """Test GET for single file"""
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")

    os.makedirs(os.path.join(upload_path, "test_project"))
    shutil.copy(
        "tests/data/test.txt",
        os.path.join(upload_path, "test_project/test.txt")
    )

    # GET file that exists
    response = test_client.get(
        "/files/v1/test.txt",
        headers=test_auth
    )

    assert response.status_code == 200
    data = json.loads(response.data)

    assert data["file_path"] == "/test.txt"
    assert data["md5"] == "150b62e4e7d58c70503bd5fc8a26463c"

    # GET file with user test2, which is in the same project
    response = test_client.get(
        "/files/v1/test.txt",
        headers=test2_auth
    )
    assert response.status_code == 200

    # GET file with user admin, which is not in the same project
    response = test_client.get(
        "/files/v1/test.txt",
        headers=admin_auth
    )
    assert response.status_code == 404


def test_delete_file(app, test_auth, monkeypatch):
    """Test DELETE for single file"""
    # Mock Metax
    monkeypatch.setattr(md, "MetaxClient", lambda: MockMetax())

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    fpath = os.path.join(upload_path, "test_project/test.txt")

    os.makedirs(os.path.join(upload_path, "test_project"))
    shutil.copy("tests/data/test.txt", fpath)

    # DELETE file that exists
    response = test_client.delete(
        "/files/v1/test.txt",
        headers=test_auth
    )

    assert response.status_code == 200
    assert json.loads(response.data)["metax"] == "/test.txt"
    assert not os.path.isfile(fpath)

    # DELETE file that does not exist
    response = test_client.delete(
        "/files/v1/test.txt",
        headers=test_auth
    )
    assert response.status_code == 404


def test_get_files(app, test_auth):
    """Test GET for the whole project"""
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

    response = test_client.get(
        "/files/v1",
        headers=test_auth
    )

    assert response.status_code == 200
    data = json.loads(response.data)

    assert data["/"] == ["test1.txt"]
    assert data["/test"] == ["test2.txt"]


def test_delete_files(app, test_auth, monkeypatch):
    """Test DELETE for the whole project"""
    # Mock Metax
    monkeypatch.setattr(md, "MetaxClient", lambda: MockMetax())

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    fpath = os.path.join(upload_path, "test_project/test.txt")

    os.makedirs(os.path.join(upload_path, "test_project"))
    shutil.copy("tests/data/test.txt", fpath)

    # DELETE the project
    response = test_client.delete(
        "/files/v1",
        headers=test_auth
    )

    assert response.status_code == 200
    assert json.loads(response.data)["metax"] == ["/test.txt"]
    assert not os.path.exists(os.path.split(fpath)[0])

    # DELETE project that does not exist
    response = test_client.delete(
        "/files/v1",
        headers=test_auth
    )
    assert response.status_code == 404


def test_db_access_test_user(app, test_auth):
    """Test database access with some other user than admin"""
    test_client = app.test_client()

    response = test_client.get("/db/v1/user", headers=test_auth)
    assert response.status_code == 401

    response = test_client.post(
        "/db/v1/user/user_project", headers=test_auth
    )
    assert response.status_code == 401

    response = test_client.delete("/db/v1/user", headers=test_auth)
    assert response.status_code == 401


def test_get_user(app, admin_auth):
    """Test get_user() function"""
    test_client = app.test_client()

    # Existing user
    response = test_client.get("/db/v1/test", headers=admin_auth)
    data = json.loads(response.data)
    assert data["_id"] == "test"
    assert response.status_code == 200

    # User that does not exist
    response = test_client.get("/db/v1/user", headers=admin_auth)
    assert response.status_code == 404


def test_get_all_users(app, admin_auth):
    """Test get_all_users() function"""
    test_client = app.test_client()

    response = test_client.get("/db/v1", headers=admin_auth)
    data = json.loads(response.data)
    assert data["users"] == ["admin", "test", "test2"]


def test_create_user(app, admin_auth, database_fx):
    """Test creating a new user"""
    test_client = app.test_client()

    # Create user that exists
    response = test_client.post(
        "/db/v1/test/test_project", headers=admin_auth
    )
    assert response.status_code == 409

    # Create user that does not exist
    response = test_client.post(
        "/db/v1/user/user_project", headers=admin_auth
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
    response = test_client.delete("/db/v1/test", headers=admin_auth)
    data = json.loads(response.data)

    assert response.status_code == 200
    assert data["username"] == "test"
    assert data["status"] == "deleted"

    # Check that user was deleted from the database
    users = database_fx.upload.users
    assert users.find_one({"_id": "test"}) is None
