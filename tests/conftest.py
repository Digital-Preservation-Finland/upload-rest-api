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

@pytest.fixture(scope="function")
def app():
    """Creates temporary upload directory and app, which uses it.
    Temp dirs are cleaned after use.

    :returns: flask.Flask instance
    """
    flask_app = create_app()
    temp_path = tempfile.mkdtemp(prefix="tests.testpath.")

    flask_app.config["TESTING"] = True
    flask_app.config["UPLOAD_PATH"] = temp_path

    yield flask_app

    # Cleanup
    shutil.rmtree(temp_path)


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
    yield {"Authorization": "Basic %s" % b64encode("test:test")}


@pytest.fixture(scope="function")
def wrong_auth():
    yield {"Authorization": "Basic %s" % b64encode("admin:test")}
