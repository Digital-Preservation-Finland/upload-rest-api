from __future__ import unicode_literals

import json
import logging
import os.path

from requests.exceptions import HTTPError

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
import upload_rest_api.utils as utils
from metax_access import MetaxError
from upload_rest_api.config import CONFIG
from upload_rest_api.jobs.utils import api_background_job


@api_background_job
def post_metadata(fpath, username, storage_id, task_id):
    """This function creates the metadata in Metax for the file(s) denoted
    by fpath argument. Finally updates the status of the task into database.

    :param str fpath: file path
    :param str username: current user
    :param str storage_id: pas storage identifier in Metax
    :param str task_id: mongo dentifier of the task
    """
    root_upload_path = CONFIG["UPLOAD_PATH"]

    status = "error"
    response = None

    metax_client = md.MetaxClient()
    database = db.Database()

    project = database.user(username).get_project()

    fpath, fname = utils.get_upload_path(project, fpath, root_upload_path)
    fpath = os.path.join(fpath, fname)
    ret_path = utils.get_return_path(project, fpath, root_upload_path)

    database.tasks.update_message(
        task_id, "Creating metadata: %s" % ret_path
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
        response = {"code": 404, "error": "File not found"}
    if not response:
        status_code = 200
        try:
            response = metax_client.post_metadata(fpaths, root_upload_path,
                                                  username, storage_id)
            status = "done"
        except HTTPError as error:
            logging.error(str(error), exc_info=error)
            response = error.response.json()
            status_code = error.response.status_code

        # Create upload-rest-api response
        response = {"code": status_code, "metax_response": response}

    database.tasks.update_status(task_id, status)
    database.tasks.update_message(task_id, json.dumps(response))


@api_background_job
def delete_metadata(fpath, username, task_id):
    """This function deletes the metadata in Metax for the file(s) denoted
    by fpath argument. Finally updates the status of the task into database.

    :param str fpath: file path
    :param str username: current user
    :param str task_id: mongo dentifier of the task
    """
    root_upload_path = CONFIG["UPLOAD_PATH"]

    status = "error"
    response = None

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
        response = {"code": 404, "error": "File not found"}

    if not response:
        try:
            response = delete_func(project, fpath, root_upload_path,
                                   force=True)
        except HTTPError as error:
            logging.error(str(error), exc_info=error)
            response = {
                "file_path": utils.get_return_path(
                    project, fpath, root_upload_path
                ),
                "metax": error.response.json()
            }
        except md.MetaxClientError as error:
            logging.error(str(error), exc_info=error)
            response = {"code": 400, "error": str(error)}
        else:
            status = "done"
            response = {
                "file_path": utils.get_return_path(
                    project, fpath, root_upload_path
                ),
                "metax": response
            }
    database.tasks.update_status(task_id, status)
    database.tasks.update_message(task_id, json.dumps(response))
