"""Tests for ``upload_rest_api.app`` module"""

import os
import shutil
import json


def _contains_symlinks(fpath):
    """Check if fpath or any subdirectories contains symlinks

    :param fpath: Path to directory to check
    :returns: True if any symlinks are found else False
    """
    for root, dirs, files in os.walk(fpath):
        for _file in files:
            if os.path.islink("%s/%s" % (root, _file)):
                return True

    return False


def test_index(app, auth, wrong_auth):
    """Test the application index page with correct
    and incorrect credentials.
    """
    test_client = app.test_client()

    response = test_client.get("/", headers=auth)
    assert response.status_code == 404

    response = test_client.get("/", headers=wrong_auth)
    assert response.status_code == 401


def test_upload(app, auth):
    """Test uploading a plain text file
    """
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")

    with open("tests/data/test.txt", "rb") as test_file:
        data = {"file": (test_file, "test.txt")}

        response = test_client.post(
            "/api/upload/v1/test.txt",
            content_type="multipart/form-data",
            data=data,
            headers=auth
        )

        assert response.status_code == 200

        fpath = os.path.join(upload_path, "test/test.txt")
        assert os.path.isfile(fpath)
        assert "test" in open(fpath).read()


def test_upload_outside_project(app, auth):
    """Test uploading outside the project folder.
    """
    test_client = app.test_client()

    with open("tests/data/test.txt", "rb") as test_file:
        data = {"file": (test_file, "test.txt")}

        response = test_client.post(
            "/api/upload/v1/project/../../test.txt",
            content_type="multipart/form-data",
            data=data,
            headers=auth
        )

    assert response.status_code == 404


def test_upload_zip(app, auth):
    """Test that uploaded zip files are extracted. No files should be
    extracted outside the project directory.
    """
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")

    with open("tests/data/test.zip", "rb") as test_file:
        data = {"file": (test_file, "test.zip")}

        response = test_client.post(
            "/api/upload/v1/test.zip",
            content_type="multipart/form-data",
            data=data,
            headers=auth
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


def test_get_file(app, auth):
    """Test GET for single file
    """
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
        headers=auth
    )

    assert response.status_code == 200
    data = json.loads(response.data)

    assert data["file_path"] == "/test/test.txt"
    assert data["md5"] == "150b62e4e7d58c70503bd5fc8a26463c"

    # GET file that does not exist
    response = test_client.get(
        "/api/upload/v1/test2.txt",
        headers=auth
    )
    assert response.status_code == 404


def test_delete_file(app, auth):
    """Test DELETE for single file
    """
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    fpath = os.path.join(upload_path, "test/test.txt")

    os.makedirs(os.path.join(upload_path, "test"))
    shutil.copy("tests/data/test.txt", fpath)

    # DELETE file that exists
    response = test_client.delete(
        "/api/upload/v1/test.txt",
        headers=auth
    )

    assert response.status_code == 200
    assert not os.path.isfile(fpath)

    # DELETE file that does not exist
    response = test_client.delete(
        "/api/upload/v1/test.txt",
        headers=auth
    )
    assert response.status_code == 404


def test_get_files(app, auth):
    """Test GET for the whole project
    """
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
        headers=auth
    )

    assert response.status_code == 200
    data = json.loads(response.data)

    assert data["/test"] == ["test1.txt"]
    assert data["/test/test"] == ["test2.txt"]


def test_delete_files(app, auth):
    """Test DELETE for the whole project
    """
    test_client = app.test_client()
    upload_path = app.config.get("UPLOAD_PATH")
    fpath = os.path.join(upload_path, "test/test.txt")

    os.makedirs(os.path.join(upload_path, "test"))
    shutil.copy("tests/data/test.txt", fpath)

    # DELETE the project
    response = test_client.delete(
        "/api/upload/v1",
        headers=auth
    )

    assert response.status_code == 200
    assert not os.path.exists(os.path.split(fpath)[0])

    # DELETE project that does not exist
    response = test_client.delete(
        "/api/upload/v1",
        headers=auth
    )
    assert response.status_code == 404
