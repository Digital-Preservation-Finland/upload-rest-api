"""Background task utility functions."""
from functools import wraps

from rq import Queue

from upload_rest_api.config import CONFIG
from upload_rest_api.database import Task, TaskStatus, get_redis_connection

FILES_QUEUE = "files"
UPLOAD_QUEUE = "upload"
JOB_QUEUE_NAMES = (FILES_QUEUE, UPLOAD_QUEUE)

# Maximum execution time for a job
DEFAULT_JOB_TIMEOUT = 12 * 60 * 60  # 12 hours
# For how long failed jobs are preserved
# NOTE: This configuration parameter is ignored in RQ versions prior to
# v1.0
DEFAULT_FAILED_JOB_TTL = 7 * 24 * 60 * 60  # 7 days


class BackgroundJobQueue(Queue):
    """Background job queue."""
    # Custom queue class for background jobs. This can be extended in
    # the future if needed.


class ClientError(Exception):
    """Exception caused by client error."""

    def __init__(self, message, files=None):
        """Init ClientError."""
        super().__init__(message)
        self.message = message
        self.files = files


def api_background_job(func):
    """Decorate RQ background jobs.

    Sets task status after task has run. If the task fails, the task
    will be marked as having failed unexpectedly in the MongoDB database
    before exception handling is passed over to the RQ worker
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        task_id = kwargs["task_id"]
        task = Task.objects.get(id=task_id)

        try:
            result = func(*args, **kwargs)
        except ClientError as exception:
            task.status = TaskStatus.ERROR
            task.message = "Task failed"
            task.errors = [
                {
                    "message": exception.message,
                    "files": exception.files
                }
            ]
            task.save()
        except Exception:
            task.status = TaskStatus.ERROR
            task.message = "Internal server error"
            task.save()
            raise
        else:
            task.status = TaskStatus.DONE
            task.message = result
            task.save()
            return result

    return wrapper


def get_job_queue(queue_name):
    """Get a RQ queue instance for the given queue.

    :param str queue_name: Queue name
    """
    if queue_name not in JOB_QUEUE_NAMES:
        raise ValueError(f"Queue {queue_name} does not exist")

    redis = get_redis_connection()

    return BackgroundJobQueue(queue_name, connection=redis)


def enqueue_background_job(task_func, queue_name, project_id, job_kwargs):
    """Create a task ID and enqueue a RQ job.

    :param str task_func: Python function to run as a string to import
                          eg. "upload_rest_api.jobs.upload.extract_archive"
    :param str queue_name: Queue used to run the job
    :param str project_id: Project identifier
    :param dict job_kwargs: Keyword arguments to pass to the background
                            task
    """
    queue = get_job_queue(queue_name)

    task = Task(project_id=project_id)
    task.message = "processing"
    task.save()

    task_id = task.id

    job_kwargs["task_id"] = str(task_id)

    job_timeout = CONFIG.get("RQ_JOB_TIMEOUT", DEFAULT_JOB_TIMEOUT)

    queue.enqueue(
        task_func,
        job_id=str(task_id),
        timeout=job_timeout,  # rq 0.12.0 or older
        job_timeout=job_timeout,  # rq 0.13.0 and newer
        failure_ttl=CONFIG.get("RQ_FAILED_JOB_TTL", DEFAULT_FAILED_JOB_TTL),
        kwargs=job_kwargs
    )
    return str(task_id)
