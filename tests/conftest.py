"""Configure py.test default values and functionality"""

import os
import sys
import tempfile
import shutil

import pytest

from upload_rest_api.app import create_app

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
