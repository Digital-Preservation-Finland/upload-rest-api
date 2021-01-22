from functools import wraps

import upload_rest_api.database as db
from redis import Redis
from rq import Queue
from upload_rest_api.config import CONFIG

FILES_QUEUE = "files"
METADATA_QUEUE = "metadata"
UPLOAD_QUEUE = "upload"

JOB_QUEUE_NAMES = (FILES_QUEUE, METADATA_QUEUE, UPLOAD_QUEUE)

# Maximum execution time for a job
JOB_TIMEOUT = 12 * 60 * 60  # 12 hours
# For how long failed jobs are preserved
FAILED_JOB_TTL = 7 * 24 * 60 * 60  # 7 days


class BackgroundJobQueue(Queue):
    # Background jobs might take a very long time, so assign
    # a very high timeout
    DEFAULT_TIMEOUT = JOB_TIMEOUT


def api_background_job(func):
    """
    Decorator for RQ background jobs.

    If the task fails, the task will be marked as having failed
    unexpectedly in the MongoDB database before exception handling
    is passed over to the RQ worker
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        task_id = kwargs["task_id"]

        try:
            return func(*args, **kwargs)
        except Exception:
            tasks = db.Database().tasks
            tasks.update_status(task_id, "error")
            tasks.update_message(task_id, "Internal server error")
            raise

    return wrapper


def get_redis_connection():
    """
    Get Redis connection used for the job queue
    """
    password = CONFIG.get("REDIS_PASSWORD", None)
    redis = Redis(
        host=CONFIG["REDIS_HOST"],
        port=CONFIG["REDIS_PORT"],
        db=CONFIG["REDIS_DB"],
        password=password if password else None
    )

    return redis


def get_job_queue(queue_name):
    """
    Get a RQ queue instance for the given queue

    :param str queue_name: Queue name
    """
    if queue_name not in JOB_QUEUE_NAMES:
        raise ValueError("Queue {} does not exist".format(queue_name))

    redis = get_redis_connection()

    return BackgroundJobQueue(queue_name, connection=redis)


def enqueue_background_job(task_func, queue_name, username, job_kwargs):
    """
    Create a task ID and enqueue a RQ job

    :param str task_func: Python function to run as a string to import
                          eg. "upload_rest_api.jobs.upload.extract_archive"
    :param str queue_name: Queue used to run the job
    :param str username: Username
    :param dict job_kwargs: Keyword arguments to pass to the background task
    """
    queue = get_job_queue(queue_name)

    database = db.Database()
    project = database.user(username).get_project()
    task_id = database.tasks.create(project)
    database.tasks.update_message(task_id, "processing")

    job_kwargs["task_id"] = str(task_id)

    queue.enqueue(
        task_func,
        job_id=str(task_id),
        failure_ttl=FAILED_JOB_TTL,
        kwargs=job_kwargs
    )
    return str(task_id)
