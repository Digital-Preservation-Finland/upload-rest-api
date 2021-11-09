"""Configure py.test default values and functionality."""
import os
import pprint
import sys
from base64 import b64encode
from runpy import run_path

import pytest
import upload_rest_api.app as app_module
import upload_rest_api.database as db
from rq import SimpleWorker
from upload_rest_api.jobs.utils import get_job_queue

import fakeredis
from mongobox import MongoBox

# Prefer modules from source directory rather than from site-python
sys.path.insert(
    0, os.path.join(os.path.abspath(os.path.dirname(__file__)), '..')
)


def pytest_addoption(parser):
    """
    Add custom flag for printing all queries done during the test
    """
    parser.addoption("--log-queries", action="store_true",
                     help=("Print a list of MongoDB queries performed during "
                           "the test."))


@pytest.yield_fixture(scope="function")
def upload_tmpdir(tmpdir):
    """Temporary directory for uploads."""
    tmpdir.mkdir("upload")
    yield tmpdir.join("upload")


@pytest.yield_fixture(scope="function", autouse=True)
def mock_config(monkeypatch, upload_tmpdir):
    """Mock the generic configuration located in
    `upload_rest_api.config` that is accessible whether Flask is active
    or not.
    """
    projects_path = upload_tmpdir.join("projects")
    temp_upload_path = upload_tmpdir.join("tmp")
    temp_tus_path = upload_tmpdir.join("tus")

    projects_path.mkdir()
    temp_upload_path.mkdir()
    temp_tus_path.mkdir()

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

    monkeypatch.setitem(CONFIG, "TUS_API_SPOOL_PATH", str(temp_tus_path))

    yield CONFIG


@pytest.fixture(autouse=True)
def patch_hashing_iters(monkeypatch):
    """Run tests with only 2000 hashing iters to avoid CPU
    bottlenecking.
    """
    monkeypatch.setattr(db, "ITERATIONS", 2000)


@pytest.yield_fixture(autouse=True, scope="session")
def test_mongo():
    """
    Initialize MongoDB test instance and return MongoDB client instance for
    the database
    """
    box = MongoBox()
    box.start()

    client = box.client()
    client.PORT = box.port

    yield client

    box.stop()


@pytest.fixture(scope="function", autouse=True)
def patch_mongo(test_mongo, monkeypatch):
    """
    Monkeypatch pymongo to use the test instance and clear the database
    before each test
    """
    test_mongo.drop_database("upload")
    monkeypatch.setattr("pymongo.MongoClient", lambda *args: test_mongo)


@pytest.yield_fixture(autouse=True)
def db_logging_fx(patch_mongo, test_mongo, request):
    """
    Optionally print list of database queries made during a test.

    If --log-queries flag is provided to pytest, all the database queries made
    during the test and it's setup are printed to stdout.
    """
    test_mongo.upload.command("profile", 2)
    yield

    if request.config.getoption("--log-queries"):
        queries = []

        for entry in test_mongo.upload.system.profile.find({}):
            try:
                queries.append(
                    entry["command"]
                )
            except KeyError:
                pass

        print()
        print("{} QUERIES were sent".format(len(queries)))
        print()
        pprint.pprint(queries, indent=4)


@pytest.yield_fixture(scope="function", autouse=True)
def mock_redis(monkeypatch):
    """Patch job queue to use a mock Redis."""
    conn = fakeredis.FakeStrictRedis()

    monkeypatch.setattr(
        "upload_rest_api.database.get_redis_connection",
        lambda: conn
    )

    yield conn

    # fakeredis versions prior to v1.0 are not isolated and use a
    # singleton, making a manual flush necessary
    conn.flushall()


@pytest.fixture(scope="function")
def background_job_runner(test_auth):
    """Convenience fixture to complete background jobs based on the task
    API response received by the client.
    """
    def wrapper(test_client, queue_name, response, expect_success=True):
        """Find the RQ job corresponding to the background task and
        finish it.

        :param test_client: Flask test client
        :param str queue_name: Queue name containing the job
        :param response: Response returned to the client that contains
                         a polling URL
        :param bool expect_success: Whether to test for task success.
                                    Default is True.

        :returns: Return the task status HTTP response after the job
                  has been finished
        """
        # Get the task ID from the polling URL from the response
        # provided to the client
        polling_url = response.json["polling_url"]
        task_id = polling_url.split("/")[-1]

        # Ensure the task can be found in the correct queue and complete
        # it
        queue = get_job_queue(queue_name)
        assert task_id in queue.job_ids

        job = queue.fetch_job(task_id)

        SimpleWorker([queue], connection=queue.connection).execute_job(
            job=job, queue=queue
        )

        # Check that the task API reports the task as having finished
        response = test_client.get(polling_url, headers=test_auth)

        assert response.json["status"] != "pending"

        if expect_success:
            assert response.json["status"] == "done"

        return response

    return wrapper


def init_db(test_mongo, database):
    """Initialize user db."""
    test_mongo.drop_database("upload")

    database.projects.create("test_project", quota=1000000)
    database.projects.create("project", quota=12345678)

    # test user
    user = database.user("test")
    user.create(projects=["test_project"], password="test")

    # test2 user with same project
    user.username = "test2"
    user.create(projects=["test_project"], password="test")

    # test3 user with different project
    user.username = "test3"
    user.create(projects=["project"], password="test")


@pytest.yield_fixture(scope="function")
def app(test_mongo, mock_config, database, monkeypatch):
    """Create temporary upload directory and app, which uses it.

    Temp dirs are cleaned after use.

    :returns: flask.Flask instance
    """
    # Patch app to use default configuration file instead of global
    # configuration file (/etc/upload_rest_api.conf)
    def _mock_configure_app(app):
        """Update Flask app to use the same configuration parameters as
        `upload_rest_api.config.CONFIG`.
        """
        app.config.from_pyfile("../include/etc/upload_rest_api.conf")

    monkeypatch.setattr(app_module, "configure_app", _mock_configure_app)

    flask_app = app_module.create_app()
    init_db(test_mongo, database)

    monkeypatch.setattr("pymongo.MongoClient", lambda *args: test_mongo)

    flask_app.config["TESTING"] = True
    flask_app.config["UPLOAD_PATH"] = mock_config["UPLOAD_PATH"]
    flask_app.config["UPLOAD_TMP_PATH"] = mock_config["UPLOAD_TMP_PATH"]
    flask_app.config["TUS_API_SPOOL_PATH"] = mock_config["TUS_API_SPOOL_PATH"]

    yield flask_app


@pytest.fixture(scope="function")
def database(test_mongo):
    """
    :returns: Database instance
    :rtype: upload_rest_api.database.Database instance
    """
    return db.Database()


@pytest.yield_fixture(scope="function")
def test_client(app):
    """
    Flask test client fixture
    """
    with app.test_client() as test_client_:
        yield test_client_


@pytest.fixture(scope="function")
def user(test_mongo):
    """Initialize and return User instance with db connection through
    Mongobox.
    """
    test_user = db.Database().user("test_user")
    test_user.users = test_mongo.upload.users

    return test_user


@pytest.fixture(scope="function")
def project(database):
    """Initialize and return a project dict
    """
    return db.Database().projects.create("test_project")


@pytest.fixture(scope="function")
def files_col(test_mongo):
    """Initialize and return Files instance with db connection through
    Mongobox.
    """
    files_coll = db.Database().files
    files_coll.files = test_mongo.upload.files

    return files_coll


@pytest.fixture(scope="function")
def tasks_col(test_mongo):
    """Initialize and return Tasks instance with db connection through
    Mongobox.
    """
    tasks_col = db.Database().tasks
    tasks_col.tasks = test_mongo.upload.tasks
    tasks_col.task_messages = test_mongo.upload.task_messages

    return tasks_col


@pytest.fixture(scope="function")
def checksums_col(test_mongo):
    """Initialize and return Checksums instance with db connection through
    Mongobox.
    """
    checksums_col = db.Database().checksums
    checksums_col.checksums = test_mongo.upload.checksums

    return checksums_col


@pytest.fixture(scope="function")
def test_auth():
    """Return correct credentials header."""
    return {
        "Authorization": "Basic %s" % b64encode(b"test:test").decode("utf-8")
    }


@pytest.fixture(scope="function")
def test2_auth():
    """Return correct credentials header."""
    return {
        "Authorization": "Basic %s" % b64encode(b"test2:test").decode("utf-8")
    }


@pytest.fixture(scope="function")
def test3_auth():
    """Return correct credentials header."""
    return {
        "Authorization": "Basic %s" % b64encode(b"test3:test").decode("utf-8")
    }


@pytest.fixture(scope="function")
def wrong_auth():
    """Return incorrect credential header."""
    return {
        "Authorization": "Basic %s" % b64encode(b"admin:admin").decode("utf-8")
    }


@pytest.fixture(scope="function")
# pylint: disable=unused-argument
# usefixtures not supported in fixture functions
def user_token_auth(test_client, test_mongo, database):
    """Returns credentials header containing an user token"""
    token_data = db.Database().tokens.create(
        name="User test token",
        username="test",
        projects=["test_project", "project"],
        expiration_date=None,
        admin=False
    )
    token = token_data["token"]

    return {
        "Authorization": f"Bearer {token}"
    }


@pytest.fixture(scope="function")
def admin_auth(test_mongo, mock_config):
    """Return credentials header containing a token with admin privileges"""
    mock_config["ADMIN_TOKEN"] = "fddps-admin"

    return {
        "Authorization": "Bearer fddps-admin"
    }
