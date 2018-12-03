"""Configure py.test default values and functionality"""

import os
import sys
import tempfile
import shutil
from base64 import b64encode

import pytest
import mongobox

from upload_rest_api.app import create_app
from upload_rest_api.database import User

# Prefer modules from source directory rather than from site-python
sys.path.insert(
    0, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..')
)


def initialize_user_db(database_fx):
    """Initialize user db to have users admin and test
    with password test.
    """
    database_fx.drop_database("auth")

    admin_user = User("admin")
    test_user = User("test")

    admin_user.users = database_fx.auth.users
    test_user.users = database_fx.auth.users

    admin_user.create(password="test")
    test_user.create(password="test")


@pytest.fixture(scope="function")
def app(database_fx):
    """Creates temporary upload directory and app, which uses it.
    Temp dirs are cleaned after use.

    :returns: flask.Flask instance
    """
    initialize_user_db(database_fx)
    flask_app = create_app()
    temp_path = tempfile.mkdtemp(prefix="tests.testpath.")

    flask_app.config["TESTING"] = True
    flask_app.config["UPLOAD_PATH"] = temp_path
    flask_app.config["MONGO_HOST"] = database_fx.HOST
    flask_app.config["MONGO_PORT"] = database_fx.PORT

    yield flask_app

    # Cleanup
    shutil.rmtree(temp_path)


@pytest.fixture(scope="session")
def database_fx():
    """Test database instance"""
    box = mongobox.MongoBox()
    box.start()

    client = box.client()
    client.PORT = box.port
    client.HOST = "localhost"

    yield client

    box.stop()


@pytest.fixture(scope="function")
def user():
    """Initializes and returns User instance with db connection
    through mongobox
    """
    box = mongobox.MongoBox()
    box.start()

    client = box.client()
    client.PORT = box.port
    client.HOST = "localhost"

    test_user = User("test_user")
    test_user.users = client.authentication.users

    yield test_user

    box.stop()


@pytest.fixture(scope="function")
def auth():
    """Yield correct credentials header"""
    yield {"Authorization": "Basic %s" % b64encode("test:test")}


@pytest.fixture(scope="function")
def wrong_auth():
    """Yield incorrect credential header"""
    yield {"Authorization": "Basic %s" % b64encode("admin:admin")}
