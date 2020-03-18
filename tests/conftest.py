"""Configure py.test default values and functionality"""
from __future__ import unicode_literals

import os
import sys
import tempfile
import shutil
from base64 import b64encode
from runpy import run_path
from concurrent.futures import ThreadPoolExecutor

import pytest
import mongomock

import upload_rest_api.app as app_module
import upload_rest_api.database as db

# Prefer modules from source directory rather than from site-python
sys.path.insert(
    0, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..')
)


@pytest.fixture(autouse=True)
def parse_conf(monkeypatch):
    """Parse conf from include/etc/upload_rest_api.conf.
    """
    monkeypatch.setattr(db, "parse_conf", lambda conf: run_path("include/etc/upload_rest_api.conf"))


@pytest.fixture(autouse=True)
def patch_hashing_iters(monkeypatch):
    """Run tests with only 2000 hashing iters to avoid CPU bottlenecking"""
    monkeypatch.setattr(db, "ITERATIONS", 2000)


@pytest.fixture(autouse=True)
def mock_mongo(monkeypatch):
    """Patch pymongo.MongoClient() with mock client"""
    mongoclient = mongomock.MongoClient()
    monkeypatch.setattr('pymongo.MongoClient', lambda *args: mongoclient)
    return mongoclient


def init_db(mock_mongo):
    """Initialize user db.
    """
    mock_mongo.drop_database("upload")

    # test user
    user = db.UsersDoc("test")
    user.users = mock_mongo.upload.users
    user.create("test_project", password="test")

    # test2 user with same project
    user.username = "test2"
    user.create("test_project", password="test")

    # test3 user with different project
    user.username = "test3"
    user.create("project", password="test")


@pytest.yield_fixture(scope="function")
def app(mock_mongo, monkeypatch):
    """Creates temporary upload directory and app, which uses it.
    Temp dirs are cleaned after use.

    :returns: flask.Flask instance
    """

    # Patch app to use default configuration file instead of global
    # configuration file (/etc/upload_rest_api.conf)
    def _mock_configure_app(app):
        """Read default configuration file"""
        app.config.from_pyfile("../include/etc/upload_rest_api.conf")

    monkeypatch.setattr(app_module, "configure_app", _mock_configure_app)

    flask_app = app_module.create_app()
    init_db(mock_mongo)
    temp_path = tempfile.mkdtemp(prefix="tests.testpath.")
    projects_path = os.path.join(temp_path, "projects")
    temp_upload_path = os.path.join(temp_path, "tmp")
    os.makedirs(projects_path)
    os.makedirs(temp_upload_path)

    flask_app.config["TESTING"] = True
    flask_app.config["UPLOAD_PATH"] = projects_path
    flask_app.config["UPLOAD_TMP_PATH"] = temp_upload_path
    flask_app.config["EXTRACT_EXECUTOR"] = ThreadPoolExecutor(max_workers=2)

    yield flask_app

    # Cleanup
    shutil.rmtree(temp_path)


@pytest.fixture(scope="function")
def user(mock_mongo, monkeypatch):
    """Initializes and returns UsersDoc instance with db connection
    through mongomock
    """
    test_user = db.UsersDoc("test_user")
    test_user.users = mock_mongo.upload.users

    return test_user


@pytest.fixture(scope="function")
def files_col(mock_mongo):
    """Initializes and returns FilesCol instance with db connection
    through mongomock
    """
    files_coll = db.FilesCol()
    files_coll.files = mock_mongo.upload.files

    return files_coll


@pytest.fixture(scope="function")
def tasks_col(mock_mongo):
    """Initializes and returns  instance with db connection
    through mongomock
    """
    tasks_col = db.AsyncTaskCol()
    tasks_col.tasks = mock_mongo.upload.tasks

    return tasks_col


@pytest.fixture(scope="function")
def test_auth():
    """Return correct credentials header"""
    return {
        "Authorization": "Basic %s" % b64encode(b"test:test").decode("utf-8")
    }


@pytest.fixture(scope="function")
def test2_auth():
    """Return correct credentials header"""
    return {
        "Authorization": "Basic %s" % b64encode(b"test2:test").decode("utf-8")
    }


@pytest.fixture(scope="function")
def test3_auth():
    """Return correct credentials header"""
    return {
        "Authorization": "Basic %s" % b64encode(b"test3:test").decode("utf-8")
    }


@pytest.fixture(scope="function")
def wrong_auth():
    """Return incorrect credential header"""
    return {
        "Authorization": "Basic %s" % b64encode(b"admin:admin").decode("utf-8")
    }
