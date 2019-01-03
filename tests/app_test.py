"""Tests for ``upload_rest_api.app`` module"""

import os
import shutil
import json


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
            data=test_file,
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
        test_client, "/api/upload/v1/test.txt",
        test_auth, "tests/data/test.txt"
    )
    assert response.status_code == 200

    fpath = os.path.join(upload_path, "test/test.txt")
    assert os.path.isfile(fpath)
    assert open(fpath, "rb").read() == open("tests/data/test.txt", "rb").read()


def test_upload_max_size(app, test_auth):
    """Test uploading file larger than the supported max file size"""

    # Set max upload size to 1 byte
    app.config["MAX_CONTENT_LENGTH"] = 1
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")

    response = _upload_file(
        test_client, "/api/upload/v1/test.txt",
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

    # Test cases for different quota and used_quota values
    # quotas[i][0] == quota; quotas[i][1] == used_quota
    quotas = [
        [8, 0],       # Quota too small for the upload
        [1024, 1018], # Not enough quota remaining for the upload
        [1024, 1026]  # Quota already exceeded
    ]

    # Files to test the scenarios with
    files = ["tests/data/test.txt", "tests/data/test.zip"]

    for quota in quotas:
        for _file in files:
            _set_user_quota(users, "test", *quota)
            response = _upload_file(
                test_client, "/api/upload/v1/test",
                test_auth, _file
            )
            assert response.status_code == 413

    # Check that none of the files were actually created
    assert not os.path.isdir(os.path.join(upload_path, "test"))


def test_used_quota(app, test_auth, database_fx):
    """Test that used quota is calculated correctly"""
    test_client = app.test_client()
    users = database_fx.upload.users

    # Upload two 31B txt files
    _upload_file(
        test_client, "/api/upload/v1/test1",
        test_auth, "tests/data/test.txt"
    )
    _upload_file(
        test_client, "/api/upload/v1/test2",
        test_auth, "tests/data/test.txt"
    )
    used_quota = users.find_one({"_id": "test"})["used_quota"]
    assert used_quota == 62

    # Delete one of the files
    test_client.delete(
        "api/upload/v1/test1",
        headers=test_auth
    )
    used_quota = users.find_one({"_id": "test"})["used_quota"]
    assert used_quota == 31


def test_upload_outside(app, test_auth):
    """Test uploading outside the user's dir."""

    test_client = app.test_client()
    response = _upload_file(
        test_client, "/api/upload/v1/../test.txt",
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
        test_client, "/api/upload/v1/test.zip",
        test_auth, "tests/data/test.zip"
    )
    assert response.status_code == 200

    fpath = os.path.join(upload_path, "test")
    text_file = os.path.join(fpath, "test", "test.txt")
    zip_file = os.path.join(fpath, "test.zip")

    # test.txt is correctly extracted
    assert os.path.isfile(text_file)
    assert "test" in open(text_file).read()

    # zip file is removed
    assert not os.path.isfile(zip_file)

    # no symlinks are created
    assert not _contains_symlinks(fpath)


def test_get_file(app, test_auth):
    """Test GET for single file"""

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")

    os.makedirs(os.path.join(upload_path, "test"))
    shutil.copy(
        "tests/data/test.txt",
        os.path.join(upload_path, "test/test.txt")
    )

    # GET file that exists
    response = test_client.get(
        "/api/upload/v1/test.txt",
        headers=test_auth
    )

    assert response.status_code == 200
    data = json.loads(response.data)

    assert data["file_path"] == "/test/test.txt"
    assert data["md5"] == "150b62e4e7d58c70503bd5fc8a26463c"

    # GET file that does not exist
    response = test_client.get(
        "/api/upload/v1/test2.txt",
        headers=test_auth
    )
    assert response.status_code == 404


def test_delete_file(app, test_auth):
    """Test DELETE for single file"""

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    fpath = os.path.join(upload_path, "test/test.txt")

    os.makedirs(os.path.join(upload_path, "test"))
    shutil.copy("tests/data/test.txt", fpath)

    # DELETE file that exists
    response = test_client.delete(
        "/api/upload/v1/test.txt",
        headers=test_auth
    )

    assert response.status_code == 200
    assert not os.path.isfile(fpath)

    # DELETE file that does not exist
    response = test_client.delete(
        "/api/upload/v1/test.txt",
        headers=test_auth
    )
    assert response.status_code == 404


def test_get_files(app, test_auth):
    """Test GET for the whole project"""

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")

    os.makedirs(os.path.join(upload_path, "test/test"))
    shutil.copy(
        "tests/data/test.txt",
        os.path.join(upload_path, "test/test1.txt")
    )
    shutil.copy(
        "tests/data/test.txt",
        os.path.join(upload_path, "test/test/test2.txt")
    )

    response = test_client.get(
        "/api/upload/v1",
        headers=test_auth
    )

    assert response.status_code == 200
    data = json.loads(response.data)

    assert data["/test"] == ["test1.txt"]
    assert data["/test/test"] == ["test2.txt"]


def test_delete_files(app, test_auth):
    """Test DELETE for the whole project"""

    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    fpath = os.path.join(upload_path, "test/test.txt")

    os.makedirs(os.path.join(upload_path, "test"))
    shutil.copy("tests/data/test.txt", fpath)

    # DELETE the project
    response = test_client.delete(
        "/api/upload/v1",
        headers=test_auth
    )

    assert response.status_code == 200
    assert not os.path.exists(os.path.split(fpath)[0])

    # DELETE project that does not exist
    response = test_client.delete(
        "/api/upload/v1",
        headers=test_auth
    )
    assert response.status_code == 404


def test_db_access_test_user(app, test_auth):
    """Test database access with some other user than admin"""

    test_client = app.test_client()

    response = test_client.get("/api/db/v1/user", headers=test_auth)
    assert response.status_code == 401

    response = test_client.post(
        "/api/db/v1/user/user_project", headers=test_auth
    )
    assert response.status_code == 401

    response = test_client.delete("/api/db/v1/user", headers=test_auth)
    assert response.status_code == 401


def test_get_user(app, admin_auth):
    """Test get_user() function"""

    test_client = app.test_client()

    # Existing user
    response = test_client.get("/api/db/v1/test", headers=admin_auth)
    data = json.loads(response.data)
    assert data["_id"] == "test"
    assert response.status_code == 200

    # User that does not exist
    response = test_client.get("/api/db/v1/user", headers=admin_auth)
    assert response.status_code == 404


def test_create_user(app, admin_auth, database_fx):
    """Test creating a new user"""

    test_client = app.test_client()

    # Create user that exists
    response = test_client.post(
        "/api/db/v1/test/test_project", headers=admin_auth
    )
    assert response.status_code == 405

    # Create user that does not exist
    response = test_client.post(
        "/api/db/v1/user/user_project", headers=admin_auth
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
    response = test_client.delete("/api/db/v1/test", headers=admin_auth)
    data = json.loads(response.data)

    assert response.status_code == 200
    assert data["username"] == "test"
    assert data["status"] == "deleted"

    # Check that user was deleted from the database
    users = database_fx.upload.users
    assert users.find_one({"_id": "test"}) is None
