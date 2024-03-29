"""Task model."""
import time

from rq.exceptions import NoSuchJobError
from rq.job import Job

from upload_rest_api.models.task_entry import TaskEntry, TaskStatus
from upload_rest_api.redis import get_redis_connection


MISSING = object()


class Task:
    """Background task"""
    def __init__(self, db_task):
        self._db_task = db_task

    # Read-only properties for database fields
    id = property(lambda x: x._db_task.id)
    project_id = property(lambda x: x._db_task.project_id)
    timestamp = property(lambda x: x._db_task.timestamp)
    status = property(lambda x: x._db_task.status)
    message = property(lambda x: x._db_task.message)
    errors = property(lambda x: x._db_task.errors)

    DoesNotExist = TaskEntry.DoesNotExist

    @classmethod
    def create(cls, project_id, message, identifier=None):
        """
        Create a new background task database entry

        :param str project_id: Project identifier
        :param str message: Initial status message
        :pram str identifier: Optional task identifier. Identifier will be
                              created automatically if not provided.

        :returns: Task instance
        """
        task_fields = {
            "project_id": project_id,
            "message": message
        }

        if identifier:
            task_fields["id"] = identifier

        db_task = TaskEntry(**task_fields)
        db_task.save()
        task = cls(db_task=db_task)

        return task

    @classmethod
    def get(cls, *args, **kwargs):
        """
        Retrieve an existing task.

        Retrieves task information from database. Also checks the state
        of related job in RQ and synchronizes the states for both if
        necessary.

        :param kwargs: Field arguments used to retrieve the task

        :returns: Task instance
        """

        task_entry = TaskEntry.objects.get(**kwargs)

        task_id = str(task_entry.id)

        try:
            job = Job.fetch(task_id, connection=get_redis_connection())
        except NoSuchJobError:
            job = None

        # If the job has failed, update the status accordingly before
        # returning it to the user.
        if job and job.is_failed and task_entry.status is not TaskStatus.ERROR:
            task_entry.status = TaskStatus.ERROR
            task_entry.message = "Internal server error"
            task_entry.save()

        return Task(
            db_task=task_entry
        )

    def set_fields(self, status=MISSING, message=MISSING, errors=MISSING):
        """
        Set various task fields

        :param TaskStatus status: Task status
        :param str message: Task message
        :param errors: Task errors, if any
        """
        # Sentinel value is used to ensure None can also be passed as a
        # valid value.
        if status is not MISSING:
            self._db_task.status = status

        if message is not MISSING:
            self._db_task.message = message

        if errors is not MISSING:
            self._db_task.errors = errors

        self._db_task.save()

    def delete(self):
        """Delete the task."""
        self._db_task.delete()

    @classmethod
    def clean_old_tasks(cls, age):
        """Delete old tasks.

        Delete tasks that are older than specified age.

        :param age: Age of task (seconds)
        """
        current_time = time.time()
        for task in TaskEntry.objects:
            if current_time - task.timestamp > age:
                task.delete()
