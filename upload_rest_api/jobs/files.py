"""Directory model background jobs."""
from upload_rest_api.jobs.utils import api_background_job
from upload_rest_api.lock import ProjectLockManager
from upload_rest_api.models.resource import Directory
from upload_rest_api.models.project import Project


@api_background_job
def delete_directory(project_id, path, task):
    """Delete a directory.

    :param str project_id: project identifier
    :param pathlib.Path path: path of the directory
    :param str task: Task instance
    """
    task.set_fields(
        message=f"Deleting files and metadata: {path}"
    )
    project = Project.get(id=project_id)
    directory = Directory(project, path)
    directory.delete()

    # Release the lock we've held from the time this background job was
    # enqueued
    lock_manager = ProjectLockManager()
    lock_manager.release(project_id, project.directory / path.strip('/'))

    return f"Deleted files and metadata: {path}"
