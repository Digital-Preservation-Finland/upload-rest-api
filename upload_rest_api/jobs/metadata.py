"""Metadata API background jobs."""
import os.path
import pathlib
import shutil
import uuid

from metax_access import ResourceAlreadyExistsError

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
from upload_rest_api.config import CONFIG
from upload_rest_api.jobs.utils import ClientError, api_background_job
from upload_rest_api.lock import ProjectLockManager


@api_background_job
def post_metadata(project_id, tmp_path, path, task_id):
    """Create file metadata in Metax.

    This function creates the metadata in Metax for the file or
    directory denoted by path argument.

    :param str project_id: project identifier
    :param str tmp_path: path to source file/directory
    :param str path: target path of file/directory
    :param str task_id: mongo dentifier of the task
    """
    metax_client = md.MetaxClient()
    database = db.Database()

    # Extract files to temporary path
    tmp_dir = pathlib.Path(CONFIG["UPLOAD_TMP_PATH"]) / str(uuid.uuid4())
    (tmp_dir / path).parent.mkdir(parents=True)
    # TODO: extract tmp_path if it is an archive
    shutil.move(tmp_path, tmp_dir / path)

    fpath = db.Projects.get_upload_path(project_id, path)
    return_path = db.Projects.get_return_path(project_id, fpath)

    database.tasks.update_message(
        task_id, f"Creating metadata: {return_path}"
    )

    try:
        fpaths = []
        for dirpath, _, files in os.walk(tmp_dir / path):
            for fname in files:
                fpaths.append(os.path.join(dirpath, fname))

        try:
            metax_client.post_metadata(fpaths, tmp_dir, project_id)
        except ResourceAlreadyExistsError as error:
            try:
                failed_files = [file_['object']['file_path']
                                for file_ in error.response.json()['failed']]
            except KeyError:
                # Most likely only one file was posted so Metax response
                # format is different
                failed_files = [return_path]
            raise ClientError(error.message, files=failed_files) from error

        # Move file to storage
        storage_path = db.Projects.get_upload_path(project_id, path)
        storage_path.parent.mkdir(exist_ok=True, parents=True)
        shutil.move(tmp_dir / path, storage_path)

        # TODO: Write permission for group is required by packaging
        # service (see https://jira.ci.csc.fi/browse/TPASPKT-516)
        os.chmod(storage_path, 0o664)

        # Remove temporary directory
        for parent in pathlib.Path(path).parents:
            print(parent)
            print(tmp_dir / parent)
            (tmp_dir / parent).rmdir()

        # Update quota
        database.projects.update_used_quota(
            project_id, CONFIG["UPLOAD_PROJECTS_PATH"]
        )
    finally:
        lock_manager = ProjectLockManager()
        lock_manager.release(project_id, fpath)

    return f"Metadata created: {return_path}"


@api_background_job
def delete_metadata(fpath, project_id, task_id):
    """Delete file metadata.

    This function deletes the metadata in Metax for the file(s) denoted
    by fpath argument.

    :param str fpath: file path
    :param str project_id: project identifier
    :param str task_id: mongo dentifier of the task
    """
    root_upload_path = CONFIG["UPLOAD_PROJECTS_PATH"]

    metax_client = md.MetaxClient()
    database = db.Database()

    fpath = db.Projects.get_upload_path(project_id, fpath)
    ret_path = db.Projects.get_return_path(project_id, fpath)
    database.tasks.update_message(
        task_id, "Deleting metadata: %s" % ret_path
    )

    try:
        if os.path.isfile(fpath):
            # Remove metadata from Metax
            delete_func = metax_client.delete_file_metadata
        elif os.path.isdir(fpath):
            # Remove all file metadata of files under dir fpath from Metax
            delete_func = metax_client.delete_all_metadata
        else:
            raise ClientError("File not found")

        try:
            response = delete_func(
                project_id, fpath, root_upload_path, force=True
            )
        except md.MetaxClientError as error:
            raise ClientError(str(error)) from error
    finally:
        lock_manager = ProjectLockManager()
        lock_manager.release(project_id, fpath)

    return "{} files deleted".format(response['deleted_files_count'])
