"""Metadata API background jobs."""
import os.path

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
import upload_rest_api.utils as utils
from metax_access import ResourceAlreadyExistsError
from upload_rest_api.config import CONFIG
from upload_rest_api.jobs.utils import ClientError, api_background_job
from upload_rest_api.lock import ProjectLockManager


@api_background_job
def post_metadata(path, project_id, storage_id, task_id):
    """Create file metadata in Metax.

    This function creates the metadata in Metax for the file or
    directory denoted by path argument.

    :param str path: relative path to file/directory
    :param str project_id: project identifier
    :param str storage_id: pas storage identifier in Metax
    :param str task_id: mongo dentifier of the task
    """
    root_upload_path = CONFIG["UPLOAD_PROJECTS_PATH"]

    metax_client = md.MetaxClient()
    database = db.Database()

    fpath = utils.get_upload_path(project_id, path)
    return_path = utils.get_return_path(project_id, fpath)

    database.tasks.update_message(
        task_id, f"Creating metadata: {return_path}"
    )

    try:
        if os.path.isdir(fpath):
            # POST metadata of all files under dir fpath
            fpaths = []
            for dirpath, _, files in os.walk(fpath):
                for fname in files:
                    fpaths.append(os.path.join(dirpath, fname))

        elif os.path.isfile(fpath):
            fpaths = [fpath]

        else:
            raise ClientError("File not found")

        try:
            metax_client.post_metadata(fpaths, root_upload_path, project_id,
                                       storage_id)
        except ResourceAlreadyExistsError as error:
            try:
                failed_files = [file_['object']['file_path']
                                for file_ in error.response.json()['failed']]
            except KeyError:
                # Most likely only one file was posted so Metax response
                # format is different
                failed_files = [return_path]
            raise ClientError(error.message, files=failed_files)
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

    fpath = utils.get_upload_path(project_id, fpath)
    ret_path = utils.get_return_path(project_id, fpath)
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
