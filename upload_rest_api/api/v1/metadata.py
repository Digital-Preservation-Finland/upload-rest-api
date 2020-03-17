"""REST api for uploading files into passipservice
"""
from __future__ import unicode_literals

import os
import json

from flask import Blueprint, safe_join, jsonify, request, current_app
from requests.exceptions import HTTPError

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
import upload_rest_api.utils as utils
from upload_rest_api.api.v1.tasks import TASK_STATUS_API_V1


METADATA_API_V1 = Blueprint("metadata_v1", __name__, url_prefix="/v1/metadata")


@utils.run_background
def post_metadata_task(metax_client, fpath, root_upload_path, username,
                       storage_id, task_id=None):
    """This function creates the metadata in Metax for the file(s) denoted
    by fpath argument. Finally updates the status of the task into database.

    :param MetaxClient metax_client: Metax access
    :param str fpath: file path
    :param str root_upload_path: Upload root directory
    :param str username: current user
    :param str task_id: mongo dentifier of the task

    :returns: The mongo identifier of the task
     """
    status = "error"
    response = None
    fpath, fname = utils.get_upload_path(fpath, root_upload_path, username)
    fpath = safe_join(fpath, fname)
    ret_path = utils.get_return_path(fpath, root_upload_path, username)
    db.AsyncTaskCol().update_message(
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
        except HTTPError as exception:
            response = exception.response.json()
            status_code = exception.response.status_code

        # Add created identifiers to Mongo
        if "success" in response and response["success"]:
            created_md = response["success"]
            db.FilesCol().store_identifiers(created_md, root_upload_path,
                                            username)

        # Create upload-rest-api response
        response = {"code": status_code, "metax_response": response}

    db.AsyncTaskCol().update_status(task_id, status)
    db.AsyncTaskCol().update_message(task_id, json.dumps(response))
    return task_id


@utils.run_background
def delete_metadata_task(metax_client, fpath, root_upload_path, username,
                         task_id=None):
    """This function deletes the metadata in Metax for the file(s) denoted
    by fpath argument. Finally updates the status of the task into database.

    :param MetaxClient metax_client: Metax access
    :param str fpath: file path
    :param str root_upload_path: Upload root directory
    :param str username: current user
    :param str task_id: mongo dentifier of the task

    :returns: The mongo identifier of the task
    """
    status = "error"
    response = None
    project = db.UsersDoc(username).get_project()
    fpath, fname = utils.get_upload_path(fpath, root_upload_path, username)
    fpath = safe_join(fpath, fname)
    ret_path = utils.get_return_path(fpath, root_upload_path, username)
    db.AsyncTaskCol().update_message(
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
        except HTTPError as exception:
            response = {
                "file_path": utils.get_return_path(fpath, root_upload_path,
                                                   username),
                "metax": exception.response.json()
            }
        except md.MetaxClientError as exception:
            response = {"code": 400, "error": str(exception)}
        else:
            status = "done"
            response = {
                "file_path": utils.get_return_path(fpath, root_upload_path,
                                                   username),
                "metax": response
            }
    db.AsyncTaskCol().update_status(task_id, status)
    db.AsyncTaskCol().update_message(task_id, json.dumps(response))
    return task_id


@METADATA_API_V1.route("/<path:fpath>", methods=["POST"])
def post_metadata(fpath):
    """POST file metadata to Metax. A background task is launched to run
    the job. The ``Location`` header and the body of the response contain
    the URL to be used for polling the status of the task. Status code is
    set to HTTP 202(Accepted).

    :returns: HTTP Response
    """
    username = request.authorization.username
    root_upload_path = current_app.config.get("UPLOAD_PATH")
    file_path, fname = utils.get_upload_path(fpath)
    file_path = safe_join(file_path, fname)

    storage_id = current_app.config.get("STORAGE_ID")
    task_id = post_metadata_task(md.MetaxClient(), fpath, root_upload_path,
                                 username, storage_id)

    polling_url = utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)
    ret_path = utils.get_return_path(file_path)
    response = jsonify({
        "file_path": ret_path,
        "message": "Creating metadata",
        "polling_url": polling_url,
        "status": "pending"
    })
    response.headers['Location'] = polling_url
    response.status_code = 202

    return response


@METADATA_API_V1.route("/<path:fpath>", methods=["DELETE"])
def delete_metadata(fpath):
    """Delete fpath metadata under user's project. A background task is
    launched to run the job. If fpath resolves to a directory metadata is
    recursively removed all the files under the directory. The ``Location``
    header and the body of the response contain the URL to be used for
    polling the status of the task. Status code is set to HTTP 202(Accepted).

    :returns: HTTP Response
    """

    root_upload_path = current_app.config.get("UPLOAD_PATH")
    username = request.authorization.username
    file_path, fname = utils.get_upload_path(fpath)
    file_path = safe_join(file_path, fname)

    task_id = delete_metadata_task(md.MetaxClient(), fpath, root_upload_path,
                                   username)

    polling_url = utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)
    ret_path = utils.get_return_path(file_path)
    response = jsonify({
        "file_path": ret_path,
        "message": "Deleting metadata",
        "polling_url": polling_url,
        "status": "pending"
    })
    response.headers['Location'] = polling_url
    response.status_code = 202

    return response
