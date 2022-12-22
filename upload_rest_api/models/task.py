"""Task model."""
from upload_rest_api.models.task_entry import TaskEntry, TaskStatus


__all__ = ('Task', 'TaskStatus')

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
