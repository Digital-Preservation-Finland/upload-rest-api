"""Configure py.test default values and functionality"""
from __future__ import unicode_literals

import os
import sys
import tempfile
import shutil
from base64 import b64encode

import pytest
import mongobox

import upload_rest_api.app as app_module
import upload_rest_api.database as db

# Prefer modules from source directory rather than from site-python
sys.path.insert(
    0, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..')
)


@pytest.fixture(autouse=True)
def patch_hashing_iters(monkeypatch):
    """Run tests with only 2000 hashing iters to avoid CPU bottlenecking"""
    monkeypatch.setattr(db, "ITERATIONS", 2000)


def init_db(database_fx):
    """Initialize user db to have users admin and test
    with password test.
    """
    database_fx.drop_database("upload")

    # ----- users collection
    user = db.UsersDoc("admin")
    user.users = database_fx.upload.users
    user.create("admin_project", password="test")

    # Test user
    user.username = "test"
    user.create("test_project", password="test")

    # Test user with same project
    user.username = "test2"
    user.create("test_project", password="test")


@pytest.yield_fixture(scope="function")
def app(database_fx, monkeypatch):
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
    init_db(database_fx)
    temp_path = tempfile.mkdtemp(prefix="tests.testpath.")

    flask_app.config["TESTING"] = True
    flask_app.config["UPLOAD_PATH"] = temp_path
    flask_app.config["MONGO_HOST"] = database_fx.HOST
    flask_app.config["MONGO_PORT"] = database_fx.PORT

    yield flask_app

    # Cleanup
    shutil.rmtree(temp_path)


@pytest.yield_fixture(scope="session")
def database_fx():
    """Test database instance"""
    box = mongobox.MongoBox()
    box.start()

    client = box.client()
    client.PORT = box.port
    client.HOST = "localhost"

    yield client

    box.stop()


@pytest.yield_fixture(scope="function")
def user():
    """Initializes and returns UsersDoc instance with db connection
    through mongobox
    """
    box = mongobox.MongoBox()
    box.start()

    client = box.client()
    client.PORT = box.port
    client.HOST = "localhost"

    test_user = db.UsersDoc("test_user")
    test_user.users = client.upload.users

    yield test_user

    box.stop()


@pytest.yield_fixture(scope="function")
def files_col():
    """Initializes and returns FilesCol instance with db connection
    through mongobox
    """
    box = mongobox.MongoBox()
    box.start()

    client = box.client()
    client.PORT = box.port
    client.HOST = "localhost"

    files_col = db.FilesCol()
    files_col.files = client.upload.files

    yield files_col

    box.stop()


@pytest.fixture(scope="function")
def test_auth():
    """Yield correct credentials header"""
    return {
        "Authorization": "Basic %s" % b64encode(b"test:test").decode("utf-8")
    }


@pytest.fixture(scope="function")
def test2_auth():
    """Yield correct credentials header"""
    return {
        "Authorization": "Basic %s" % b64encode(b"test2:test").decode("utf-8")
    }


@pytest.fixture(scope="function")
def admin_auth():
    """Yield correct credentials header"""
    return {
        "Authorization": "Basic %s" % b64encode(b"admin:test").decode("utf-8")
    }


@pytest.fixture(scope="function")
def wrong_auth():
    """Yield incorrect credential header"""
    return {
        "Authorization": "Basic %s" % b64encode(b"admin:admin").decode("utf-8")
    }
