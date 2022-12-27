"""TaskEntry class."""
import time
from enum import Enum

from bson import ObjectId
from mongoengine import (DictField, Document, EnumField, FloatField, ListField,
                         StringField)


class TaskStatus(Enum):
    """Task status for background tasks."""
    PENDING = "pending"
    ERROR = "error"
    DONE = "done"


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
    }
