"""Unit tests for Task database class"""
from bson import ObjectId

from upload_rest_api.models.task import Task, TaskEntry, TaskStatus


def test_correct_document_structure(tasks_col):
    """
    Test that saved Task has the same document structure as the pre-MongoEngine
    implementation
    """
    task = TaskEntry(
        id="6346ab9b60faf26069e92a80",
        project_id="test_project",
        timestamp=12345678.0,
        status=TaskStatus.PENDING,
        message="This is still under progress",
        errors=[
            {
                "error": "something happened"
            }
        ]
    )
    task.save()

    docs = list(tasks_col.find())
    assert len(docs) == 1

    assert docs[0] == {
        "_id": ObjectId("6346ab9b60faf26069e92a80"),
        "project": "test_project",
        "timestamp": 12345678.0,
        "status": "pending",
        "message": "This is still under progress",
        "errors": [
            {
                "error": "something happened"
            }
        ]
    }
