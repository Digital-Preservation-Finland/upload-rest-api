"""Unit tests for background jobs."""
import pytest
from rq import SimpleWorker
from upload_rest_api.jobs.utils import (api_background_job,
                                        enqueue_background_job, get_job_queue)


@api_background_job
def successful_task(task_id, value):
    """Fake background job executed by RQ."""
    return "Task ID = {}, value = {}".format(task_id, value)


@api_background_job
def failing_task(task_id):
    """Fake failing background job executed by RQ."""
    raise ValueError("Couldn't reticulate splines")


@pytest.mark.usefixtures("app")
def test_enqueue_background_job_successful(tasks_col, mock_redis):
    """Test enqueuing a fake task using "enqueue_background_job"
    and ensure it can be executed properly.
    """
    job_id = enqueue_background_job(
        task_func="tests.unit_tests.jobs_test.successful_task",
        queue_name="upload",
        username="test",
        job_kwargs={"value": "spam"}
    )

    # Ensure the job is enqueued and MongoDB entry exists
    pending_jobs = list(tasks_col.find("test_project", "pending"))
    assert len(pending_jobs) == 1

    job = pending_jobs[0]
    assert str(job["_id"]) == job_id

    assert job["project"] == "test_project"
    assert job["message"] == "processing"
    assert job["status"] == "pending"

    # Check that the Redis queue has the same job
    upload_queue = get_job_queue("upload")
    assert upload_queue.job_ids == [job_id]

    # Job can be finished
    SimpleWorker([upload_queue], connection=mock_redis).work(burst=True)

    rq_job = upload_queue.fetch_job(job_id)

    assert rq_job.result == "Task ID = {}, value = spam".format(job_id)


@pytest.mark.usefixtures("app")
def test_enqueue_background_job_failing(tasks_col, mock_redis):
    """Test enqueuing a fake task using "enqueue_background_job"
    and ensure it is handled properly if it raises an exception.
    """
    job_id = enqueue_background_job(
        task_func="tests.unit_tests.jobs_test.failing_task",
        queue_name="upload",
        username="test",
        job_kwargs={}
    )

    # Ensure the job is enqueued and MongoDB entry exists
    pending_jobs = list(tasks_col.find("test_project", "pending"))
    assert len(pending_jobs) == 1

    job = pending_jobs[0]
    assert str(job["_id"]) == job_id

    assert job["project"] == "test_project"
    assert job["message"] == "processing"
    assert job["status"] == "pending"

    # Check that the Redis queue has the same job
    upload_queue = get_job_queue("upload")
    assert upload_queue.job_ids == [job_id]

    # Job can be finished
    SimpleWorker([upload_queue], connection=mock_redis).work(burst=True)

    rq_job = upload_queue.fetch_job(job_id)

    assert rq_job.is_failed

    failed_jobs = list(tasks_col.find("test_project", "error"))
    assert len(failed_jobs) == 1

    assert failed_jobs[0]["message"] == "Internal server error"


@pytest.mark.usefixtures("app")
def test_enqueue_background_job_failing_out_of_sync(tasks_col, mock_redis):
    """Test enqueuing a fake task using "enqueue_background_job"
    and ensure it is handled properly if the failure is recorded in RQ
    but not MongoDB.
    """
    job_id = enqueue_background_job(
        task_func="tests.unit_tests.jobs_test.failing_task",
        queue_name="upload",
        username="test",
        job_kwargs={}
    )

    # Check that the Redis queue has the same job
    upload_queue = get_job_queue("upload")
    assert upload_queue.job_ids == [job_id]

    # Job can be finished
    SimpleWorker([upload_queue], connection=mock_redis).work(burst=True)

    rq_job = upload_queue.fetch_job(job_id)

    assert rq_job.is_failed

    # Update the status in MongoDB to appear in-progress, while in RQ
    # it has already failed
    tasks_col.update_message(job_id, "processing")
    tasks_col.update_status(job_id, "pending")

    # Retrieve the task from MongoDB; it should be automatically updated
    # to match the status in RQ
    task = tasks_col.get(job_id)

    assert task["message"] == "Internal server error"
    assert task["status"] == "error"


@pytest.mark.usefixtures("app", "mock_redis")
def test_enqueue_background_job_custom_timeout(mock_config, monkeypatch):
    """Test enqueueing a background job with custom timeout in effect
    and ensure it is used.
    """
    monkeypatch.setitem(mock_config, "RQ_JOB_TIMEOUT", 2222)

    job_id = enqueue_background_job(
        task_func="tests.unit_tests.jobs_test.failing_task",
        queue_name="upload",
        username="test",
        job_kwargs={}
    )

    upload_queue = get_job_queue("upload")
    job = upload_queue.fetch_job(job_id)

    assert job.timeout == 2222
