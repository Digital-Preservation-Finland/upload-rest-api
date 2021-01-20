from __future__ import unicode_literals

import pytest
from rq import SimpleWorker
from upload_rest_api.jobs.utils import enqueue_background_job, get_job_queue


def fake_task(task_id, value):
    """
    Fake background job executed by RQ
    """
    return "Task ID = {}, value = {}".format(task_id, value)


@pytest.mark.usefixtures("app")
def test_enqueue_fake_job(tasks_col, mock_redis):
    """
    Test enqueuing a fake task using "enqueue_background_job"
    and ensure it can be executed properly
    """
    job_id = enqueue_background_job(
        task_func="tests.unit_tests.jobs_test.fake_task",
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

    assert upload_queue.finished_job_registry.get_job_ids() == [job_id]
    job = upload_queue.fetch_job(job_id)

    assert job.result == "Task ID = {}, value = spam".format(job_id)
