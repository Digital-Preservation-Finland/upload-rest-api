"""Configure py.test default values and functionality"""

import os
import sys
import tempfile
import shutil
from base64 import b64encode

import pytest
import mongobox

import upload_rest_api.app as app_module
import upload_rest_api.database as db
from upload_rest_api.database import UsersDoc

# Prefer modules from source directory rather than from site-python
sys.path.insert(
    0, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..')
)


def init_db(database_fx):
    """Initialize user db to have users admin and test
    with password test.
    """
    database_fx.drop_database("upload")

    user = UsersDoc("admin")
    user.users = database_fx.upload.users
    user.create("admin_project", password="test")

    # Test user
    user.username = "test"
    user.create("test_project", password="test")

    # Test user with same project
    user.username = "test2"
    user.create("test_project", password="test")


@pytest.fixture(autouse=True)
def patch_hashing_iters(monkeypatch):
    """Run tests with only 2000 hashing iters to avoid CPU bottlenecking"""
    monkeypatch.setattr(db, "ITERATIONS", 2000)


@pytest.yield_fixture(scope="function")
def app(database_fx, monkeypatch):
    """Creates temporary upload directory and app, which uses it.
    Temp dirs are cleaned after use.

    :returns: flask.Flask instance
    """

    def _configure_app(app):
        """Test configuration for the app.
        Reads /etc/upload_rest_api.conf if the file exists or
        uses default params from include/etc/upload_rest_api.conf.
        """
        if os.path.isfile("/etc/upload_rest_api.conf"):
            app.config.from_pyfile("/etc/upload_rest_api.conf")
        else:
            app.config.from_pyfile("../include/etc/upload_rest_api.conf")


    monkeypatch.setattr(app_module, "configure_app", _configure_app)

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
    """Initializes and returns User instance with db connection
    through mongobox
    """
    box = mongobox.MongoBox()
    box.start()

    client = box.client()
    client.PORT = box.port
    client.HOST = "localhost"

    test_user = UsersDoc("test_user")
    test_user.users = client.authentication.users

    yield test_user

    box.stop()


@pytest.fixture(scope="function")
def test_auth():
    """Yield correct credentials header"""
    return {"Authorization": "Basic %s" % b64encode("test:test")}


@pytest.fixture(scope="function")
def test2_auth():
    """Yield correct credentials header"""
    return {"Authorization": "Basic %s" % b64encode("test2:test")}


@pytest.fixture(scope="function")
def admin_auth():
    """Yield correct credentials header"""
    return {"Authorization": "Basic %s" % b64encode("admin:test")}


@pytest.fixture(scope="function")
def wrong_auth():
    """Yield incorrect credential header"""
    return {"Authorization": "Basic %s" % b64encode("admin:admin")}
