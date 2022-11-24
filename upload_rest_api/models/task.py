import time
from enum import Enum

from bson import ObjectId
from mongoengine import (DictField, Document, EnumField, FloatField, ListField,
                         QuerySet, StringField)
from rq.exceptions import NoSuchJobError
from rq.job import Job

from upload_rest_api.redis import get_redis_connection

MISSING = object()


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


class TaskEntry(Document):
    """Database entry for a background task."""
    id = StringField(
        primary_key=True, required=False, default=lambda: str(ObjectId())
    )
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
        Retrieve an existing task

        :param kwargs: Field arguments used to retrieve the task

        :returns: Task instance
        """
        return Task(
            db_task=TaskEntry.objects.get(**kwargs)
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
