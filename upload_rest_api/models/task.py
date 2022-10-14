import time
from enum import Enum

from mongoengine import (DictField, Document, EnumField, FloatField, ListField,
                         QuerySet, StringField)
from rq.exceptions import NoSuchJobError
from rq.job import Job

from upload_rest_api.database import get_redis_connection


class TaskStatus(Enum):
    """Task status for background tasks."""
    PENDING = "pending"
    ERROR = "error"
    DONE = "done"


class TaskQuerySet(QuerySet):
    """
    Custom query set for Task documents that takes care of automatically
    synchronizing the state between the tasks on RQ and MongoDB.
    """
    def get(self, *args, **kwargs):
        """
        Custom getter that also checks the RQ at the same time and synchronizes
        the state for both if necessary
        """
        task = super().get(*args, **kwargs)

        task_id = str(task.id)

        try:
            job = Job.fetch(task_id, connection=get_redis_connection())
        except NoSuchJobError:
            return task

        # If the job has failed, update the status accordingly before
        # returning it to the user.
        if job.is_failed and task.status is not TaskStatus.ERROR:
            task.status = TaskStatus.ERROR
            task.message = "Internal server error"
            task.save()

        return task


class Task(Document):
    """Background task."""
    project_id = StringField(
        # The underlying field name is 'project' for backwards compatibility
        db_field="project",
        required=True
    )
    # Task UNIX timestamp
    # TODO: Convert this to use the proper date type?
    timestamp = FloatField(null=False, default=time.time)

    # Status of the task
    status = EnumField(TaskStatus, default=TaskStatus.PENDING)
    # Optional status message for the task
    message = StringField(required=False)
    errors = ListField(DictField())

    meta = {
        "collection": "tasks",
        "queryset_class": TaskQuerySet
    }
