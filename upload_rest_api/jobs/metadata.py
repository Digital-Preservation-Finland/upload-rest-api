"""Metadata API background jobs."""
import os.path

from metax_access import ResourceAlreadyExistsError

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
import upload_rest_api.utils as utils
from upload_rest_api.config import CONFIG
from upload_rest_api.jobs.utils import api_background_job, ClientError


@api_background_job
def post_metadata(fpath, username, storage_id, task_id):
    """Create file metadata in Metax.

    This function creates the metadata in Metax for the file(s) denoted
    by fpath argument. Finally updates the status of the task into
    database.

    :param str fpath: file path
    :param str username: current user
    :param str storage_id: pas storage identifier in Metax
    :param str task_id: mongo dentifier of the task
    """
    root_upload_path = CONFIG["UPLOAD_PATH"]

    metax_client = md.MetaxClient()
    database = db.Database()

    project = database.user(username).get_project()

    fpath, fname = utils.get_upload_path(project, fpath, root_upload_path)
    fpath = os.path.join(fpath, fname)
    ret_path = utils.get_return_path(project, fpath, root_upload_path)

    database.tasks.update_message(
        task_id, "Creating metadata: {}".format(ret_path)
    )

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
        metax_client.post_metadata(fpaths, root_upload_path, username,
                                   storage_id)
    except ResourceAlreadyExistsError as error:
        raise ClientError(error.message) from error

    database.tasks.update_message(task_id,
                                  "Metadata created: {}".format(ret_path))
    database.tasks.update_status(task_id, "done")


@api_background_job
def delete_metadata(fpath, username, task_id):
    """Delete file metadata.

    This function deletes the metadata in Metax for the file(s) denoted
    by fpath argument. Finally updates the status of the task into
    database.

    :param str fpath: file path
    :param str username: current user
    :param str task_id: mongo dentifier of the task
    """
    root_upload_path = CONFIG["UPLOAD_PATH"]

    metax_client = md.MetaxClient()
    database = db.Database()

    project = database.user(username).get_project()
    fpath, fname = utils.get_upload_path(project, fpath, root_upload_path)
    fpath = os.path.join(fpath, fname)
    ret_path = utils.get_return_path(project, fpath, root_upload_path)
    database.tasks.update_message(
        task_id, "Deleting metadata: %s" % ret_path
    )

    if os.path.isfile(fpath):
        # Remove metadata from Metax
        delete_func = metax_client.delete_file_metadata
    elif os.path.isdir(fpath):
        # Remove all file metadata of files under dir fpath from Metax
        delete_func = metax_client.delete_all_metadata
    else:
        raise ClientError("File not found")

    try:
        response = delete_func(project, fpath, root_upload_path, force=True)
    except md.MetaxClientError as error:
        raise ClientError(str(error)) from error

    database.tasks.update_message(
        task_id,
        "{} files deleted".format(response['deleted_files_count'])
    )
    database.tasks.update_status(task_id, "done")
