"""Tests for ``upload_rest_api.app`` module."""
from urllib.parse import urlencode

import pytest
from rq import SimpleWorker

from upload_rest_api import jobs
from upload_rest_api.models.task import TaskStatus


def test_reverse_proxy_polling_url(
        app, mock_redis, test_auth, requests_mock, tus_upload_file):
    """Test reverse proxy polling url.

    Mock the web application running behind a reverse proxy and ensure
    that the reverse proxy's URL is detected by the web application.
    """
    requests_mock.get(
        "https://metax.localdomain/v3/files?{}".format(
            urlencode({
                "pathname": "/testdir/test.txt", "csc_project": "test_project",
            })
        ),
        json={"results": [], "next": None}
    )
    requests_mock.post("https://metax.localdomain/v3/files/post-many", json={})
    requests_mock.post(
        "https://metax.localdomain/v3/files/datasets"
        "?relations=true&storage_service=pas",
        json={}
    )
    test_client = app.test_client()

    tus_upload_file(project_id="test_project", path="testdir/test.txt")

    # Add an environment variable containing the X-Forwarded-Host HTTP
    # header value.
    # This is how Werkzeug (eg. all WSGI servers) read the HTTP headers
    # for an incoming request.
    response = test_client.delete(
        "/v1/files/test_project/testdir",
        headers=test_auth,
        environ_base={"HTTP_X_FORWARDED_HOST": "reverse_proxy"}
    )
    assert response.json["polling_url"].startswith(
        "http://reverse_proxy/v1/tasks/"
    )

    # Remove lock for dangling background job
    mock_redis.flushall()


@jobs.api_background_job
def _modify_task_info(task):
    """Modify task info in database."""
    task.set_fields(
        message="foo",
        status=TaskStatus.DONE
    )
    return "baz"


@jobs.api_background_job
def _raise_general_exception(task):
    """Raise general exception."""
    raise Exception('Something failed')


@jobs.api_background_job
def _raise_client_error(task):
    """Raise ClientError."""
    raise jobs.ClientError('Client made mistake.')


@pytest.mark.parametrize(
    ('task_func', 'expected_response'),
    [
        (
            'tests.api.v1.tasks_test._modify_task_info',
            {'message': 'baz', 'status': 'done'}
        ),
        (
            'tests.api.v1.tasks_test._raise_general_exception',
            {'message': 'Internal server error', 'status': 'error'}
        ),
        (
            'tests.api.v1.tasks_test._raise_client_error',
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
        project_id="test_project",
        job_kwargs={}
    )

    # Task should be pending
    test_client = app.test_client()
    response = test_client.get(f"/v1/tasks/{job}", headers=test_auth)
    assert response.json == {'message': 'processing', 'status': 'pending'}

    # Run job. Task should be finished (status: done) or failed (status:
    # error).
    SimpleWorker([jobs.get_job_queue("upload")],
                 connection=mock_redis).work(burst=True)
    response = test_client.get(f"/v1/tasks/{job}", headers=test_auth)
    assert response.json == expected_response


def test_task_not_found(app, test_auth):
    """Test querying task that does not exists."""
    response = app.test_client().get("/v1/tasks/abcd1234abcd1234abcd1234",
                                     headers=test_auth)
    assert response.status_code == 404
    assert response.json['status'] == 'Not found'
