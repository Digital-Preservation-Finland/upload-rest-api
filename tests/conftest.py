"""Configure py.test default values and functionality"""
from __future__ import unicode_literals

import json
import os
import shutil
import sys
import tempfile
from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor
from runpy import run_path

import fakeredis
import mongomock
import pytest
import upload_rest_api.app as app_module
import upload_rest_api.database as db
from rq import SimpleWorker
from upload_rest_api.jobs.utils import get_job_queue

# Prefer modules from source directory rather than from site-python
sys.path.insert(
    0, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..')
)


@pytest.fixture(autouse=True)
def parse_conf(monkeypatch):
    """Parse conf from include/etc/upload_rest_api.conf.
    """
    monkeypatch.setattr(
        db, "parse_conf",
        lambda conf: run_path("include/etc/upload_rest_api.conf")
    )
    monkeypatch.setattr(
        "upload_rest_api.config.get_config",
        lambda: run_path("include/etc/upload_rest_api.conf")
    )


@pytest.yield_fixture(scope="function")
def upload_tmpdir(tmp_path_factory):
    """
    Temporary directory for uploads
    """
    yield tmp_path_factory.mktemp("tests.upload_")


@pytest.yield_fixture(scope="function")
def mock_config(monkeypatch, upload_tmpdir):
    """
    Mock the generic configuration located in `upload_rest_api.config` that
    is accessible whether Flask is active or not
    """
    projects_path = upload_tmpdir / "projects"
    temp_upload_path = upload_tmpdir / "tmp"

    projects_path.mkdir()
    temp_upload_path.mkdir()

    mock_config_ = run_path("include/etc/upload_rest_api.conf")

    from upload_rest_api.config import CONFIG

    # Copy the values from "include/etc/upload_rest_api.conf"
    for key, value in mock_config_.items():
        if not key[0].isupper():
            # Skip Python built-ins
            continue

        monkeypatch.setitem(CONFIG, key, value)

    monkeypatch.setitem(CONFIG, "UPLOAD_PATH", str(projects_path))
    monkeypatch.setitem(CONFIG, "UPLOAD_TMP_PATH", str(temp_upload_path))

    yield CONFIG


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


@pytest.fixture(scope="function", autouse=True)
def mock_redis(monkeypatch):
    """
    Patch job queue to use a mock Redis
    """
    server = fakeredis.FakeServer()
    conn = fakeredis.FakeStrictRedis(server=server)

    monkeypatch.setattr(
        "upload_rest_api.jobs.utils.get_redis_connection",
        lambda: conn
    )

    yield conn


@pytest.fixture(scope="function")
def background_job_runner(test_auth):
    """
    Convenience fixture to complete background jobs based on the task API
    response received by the client
    """
    def wrapper(test_client, queue_name, response, expect_success=True):
        """
        Find the RQ job corresponding to the background task and finish it

        :param test_client: Flask test client
        :param str queue_name: Queue name containing the job
        :param response: Response returned to the client that contains
                         a polling URL
        :param bool expect_success: Whether to test for task success.
                                    Default is True.

        :returns: Return the task status HTTP response after the job
                  has been finished
        """
        # Get the task ID from the polling URL from the response provided
        # to the client
        polling_url = json.loads(response.data)["polling_url"]
        task_id = polling_url.split("/")[-1]

        # Ensure the task can be found in the correct queue and complete it
        queue = get_job_queue(queue_name)
        assert task_id in queue.job_ids

        job = queue.fetch_job(task_id)

        SimpleWorker([queue], connection=queue.connection).execute_job(
            job=job, queue=queue
        )

        # Check that the task API reports the task as having finished
        response = test_client.get(polling_url, headers=test_auth)
        data = json.loads(response.data)

        assert data["status"] != "pending"

        if expect_success:
            assert data["status"] == "done"

        return response

    return wrapper


def init_db(mock_mongo):
    """Initialize user db.
    """
    mock_mongo.drop_database("upload")

    # test user
    user = db.Database().user("test")
    user.users = mock_mongo.upload.users
    user.create("test_project", password="test")

    # test2 user with same project
    user.username = "test2"
    user.create("test_project", password="test")

    # test3 user with different project
    user.username = "test3"
    user.create("project", password="test")


@pytest.yield_fixture(scope="function")
def app(mock_mongo, mock_config, monkeypatch):
    """Creates temporary upload directory and app, which uses it.
    Temp dirs are cleaned after use.

    :returns: flask.Flask instance
    """

    # Patch app to use default configuration file instead of global
    # configuration file (/etc/upload_rest_api.conf)
    def _mock_configure_app(app):
        """
        Update current_app.config to reference the same flask.Config
        instance as `upload_rest_api.config.CONFIG`
        """
        app.config.from_pyfile("../include/etc/upload_rest_api.conf")

    monkeypatch.setattr(app_module, "configure_app", _mock_configure_app)

    flask_app = app_module.create_app()
    init_db(mock_mongo)

    flask_app.config["TESTING"] = True
    flask_app.config["UPLOAD_PATH"] = mock_config["UPLOAD_PATH"]
    flask_app.config["UPLOAD_TMP_PATH"] = mock_config["UPLOAD_TMP_PATH"]
    flask_app.config["EXTRACT_EXECUTOR"] = ThreadPoolExecutor(max_workers=2)

    yield flask_app


@pytest.fixture(scope="function")
def user(mock_mongo):
    """Initializes and returns User instance with db connection
    through mongomock
    """
    test_user = db.Database().user("test_user")
    test_user.users = mock_mongo.upload.users

    return test_user


@pytest.fixture(scope="function")
def files_col(mock_mongo):
    """Initializes and returns Files instance with db connection
    through mongomock
    """
    files_coll = db.Database().files
    files_coll.files = mock_mongo.upload.files

    return files_coll


@pytest.fixture(scope="function")
def tasks_col(mock_mongo):
    """Initializes and returns  instance with db connection
    through mongomock
    """
    tasks_col = db.Database().tasks
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
