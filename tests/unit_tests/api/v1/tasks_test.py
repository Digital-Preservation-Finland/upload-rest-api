"""Tests for ``upload_rest_api.app`` module."""
import pytest
from rq import SimpleWorker

import upload_rest_api.database as database
import upload_rest_api.jobs as jobs


def test_reverse_proxy_polling_url(app, test_auth):
    """Mock the web application running behind a reverse proxy and ensure that
    the reverse proxy's URL is detected by the web application.
    """
    test_client = app.test_client()

    # Add an environment variable containing the X-Forwarded-Host HTTP header
    # value.
    # This is how Werkzeug (eg. all WSGI servers) read the HTTP headers for an
    # incoming request.
    response = test_client.post(
        "/v1/metadata/*",
        headers=test_auth,
        environ_base={"HTTP_X_FORWARDED_HOST": "reverse_proxy"}
    )
    assert response.json["polling_url"].startswith(
        "http://reverse_proxy/v1/tasks/"
    )


@jobs.api_background_job
def _modify_task_info(task_id):
    """Modify task info in database."""
    tasks = database.Database().tasks
    tasks.update_message(task_id, "foo")
    tasks.update_status(task_id, "bar")
    return "baz"


@jobs.api_background_job
def _raise_general_exception(task_id):
    """Raise general exception."""
    raise Exception('Something failed')


@jobs.api_background_job
def _raise_client_error(task_id):
    """Raise ClientError."""
    raise jobs.ClientError('Client made mistake.')


@pytest.mark.parametrize(
    ('task_func', 'expected_response'),
    [
        (
            'tests.unit_tests.api.v1.tasks_test._modify_task_info',
            {'message': 'baz', 'status': 'done'}
        ),
        (
            'tests.unit_tests.api.v1.tasks_test._raise_general_exception',
            {'message': 'Internal server error', 'status': 'error'}
        ),
        (
            'tests.unit_tests.api.v1.tasks_test._raise_client_error',
            {
                'message': 'Task failed',
                'errors': [{'message': 'Client made mistake.', 'files': None}],
                'status': 'error'}
        )
    ]
)
def test_query_task(app, mock_redis, test_auth, task_func, expected_response):
    """Test querying task status.

    :param app: Flask app
    :param mock_redis: Redis mocker
    :param test_auth: authentication headers
    :param task_func: function to be queued in RQ
    :param expected_response: expected API JSON response
    """
    # Enqueue a job
    job = jobs.enqueue_background_job(
        task_func=task_func,
        queue_name="upload",
        username="test",
        job_kwargs={}
    )

    # Task should be pending
    test_client = app.test_client()
    response = test_client.get("/v1/tasks/{}".format(job), headers=test_auth)
    assert response.json == {'message': 'processing', 'status': 'pending'}

    # Run job. Task should be finished (status: done) or failed (status:
    # error).
    SimpleWorker([jobs.get_job_queue("upload")],
                 connection=mock_redis).work(burst=True)
    response = test_client.get("/v1/tasks/{}".format(job), headers=test_auth)
    assert response.json == expected_response


def test_task_not_found(app, test_auth):
    """Test querying task that does not exists."""
    response = app.test_client().get("/v1/tasks/abcd1234abcd1234abcd1234",
                                     headers=test_auth)
    assert response.status_code == 404
    assert response.json['status'] == 'Not found'
