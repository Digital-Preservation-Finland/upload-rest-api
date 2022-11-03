"""FileEntrys API background jobs."""
import os
import shutil

import upload_rest_api.gen_metadata as md
from upload_rest_api.models import FileEntry, Project, Task
from upload_rest_api.jobs.utils import api_background_job
from upload_rest_api.lock import ProjectLockManager


def _get_files_to_delete(trash_path, trash_root, project_dir):
    """
    Get files to delete from the files database collections.

    Each path is converted to the original absolute path in the format
    "/var/spool/upload/projects/<project_id>". This is necessary because
    the database uses absolute paths as identifiers.
    """
    filepaths = []
    for _dir, _, files in os.walk(trash_path):
        for _file in files:
            fpath = os.path.join(_dir, _file)
            fpath = os.path.abspath(fpath)

            # Change the path
            # from `<spool_path>/trash/<trash_id>/<project_id>`
            # to `<spool_path>/upload/<project_id>`
            fpath = f"{project_dir}{fpath[len(str(trash_root)):]}"
            filepaths.append(fpath)

    return filepaths


@api_background_job
def delete_files(fpath, trash_path, trash_root, project_id, task_id):
    """Delete files and metadata denoted by fpath directory under temporary
    directory. The whole directory is recursively removed after Metax metadata
    is removed.

    :param pathlib.Path fpath: path to the original directory
    :param pathlib.Path trash_path: path to the temporary trash directory
    :param pathlib.Path trash_root: root of the temporary trash directory,
                                    corresponding to
                                    `<spool_path>/<trash_id>/<project_id>`
    :param str project: project identifier
    :param str task_id: mongo dentifier of the task
    """
    # Remove metadata from Metax
    metax_client = md.MetaxClient()
    project_dir = Project.get_project_directory(project_id)
    ret_path = Project.get_return_path(project_id, fpath)

    Task.objects.filter(id=task_id).update(
        message=f"Deleting files and metadata: {ret_path}"
    )

    metax_client.delete_all_metadata(
        project=project_id,
        fpath=trash_path,
        # Provide the root path *without* the project directory
        # as the leading part
        root_upload_path=trash_root.parent
    )

    # Remove files from Mongo
    files_to_delete = _get_files_to_delete(
        trash_path=trash_path,
        trash_root=trash_root,
        project_dir=project_dir
    )
    FileEntry.objects.bulk_delete_by_paths(files_to_delete)

    # Ensure the directory containing the "fake" project directory is
    # deleted as well.
    shutil.rmtree(trash_root.parent)

    # Update used_quota
    project = Project.objects.get(id=project_id)
    project.update_used_quota()

    # Release the lock we've held from the time this background job was
    # enqueued
    lock_manager = ProjectLockManager()
    lock_manager.release(project_id, fpath)

    return f"Deleted files and metadata: {ret_path}"
