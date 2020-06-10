"""/files/v1 endpoints. Functionality for uploading,
querying and deleting files from the server.
"""
from __future__ import unicode_literals

import os
import json
import logging
from shutil import rmtree

from flask import Blueprint, safe_join, request, jsonify, current_app, url_for
from werkzeug.utils import secure_filename
from requests.exceptions import HTTPError

from metax_access import MetaxError

import upload_rest_api.upload as up
import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
import upload_rest_api.utils as utils
from upload_rest_api.api.v1.tasks import TASK_STATUS_API_V1

FILES_API_V1 = Blueprint("files_v1", __name__, url_prefix="/v1/files")


def _get_dir_tree(project, fpath, root_upload_path):
    """Returns with dir tree from fpath as a dict"""
    file_dict = {}
    for dirpath, _, files in os.walk(fpath):
        path = utils.get_return_path(project, dirpath, root_upload_path)
        file_dict[path] = files

    if "." in file_dict:
        file_dict["/"] = file_dict.pop(".")

    return file_dict


def _delete(metax_client, fpath, root_upload_path, username, task_id):
    """Delete files and metadata"""
    # Remove metadata from Metax
    database = db.Database()
    project = database.user(username).get_project()
    ret_path = utils.get_return_path(project, fpath, root_upload_path)
    database.tasks.update_message(
        task_id,
        "Deleting files and metadata: %s" % ret_path
    )
    try:
        metax_response = metax_client.delete_all_metadata(project, fpath,
                                                          root_upload_path)
    except (MetaxError, HTTPError) as error:
        logging.error(str(error), exc_info=error)
        database.tasks.update_status(task_id, "error")
        msg = {"message": str(error)}
        database.tasks.update_message(task_id, json.dumps(msg))
    else:
        # Remove checksum from mongo
        database.checksums.delete_dir(fpath)

        # Remove project directory and update used_quota
        rmtree(fpath)
        database.user(username).update_used_quota(root_upload_path)
        database.tasks.update_status(task_id, "done")
        response = {
            "file_path": ret_path,
            "status": "done",
            "metax": metax_response
        }
        database.tasks.update_message(task_id, json.dumps(response))


@utils.run_background
def delete_task(metax_client, fpath, root_upload_path, username, task_id=None):
    """Deletes files and metadata denoted by fpath directory under user's
    project. The whole directory is recursively removed.

    :param MetaxClient metax_client: Metax access
    :param str fpath: path to directory
    :param str root_upload_path: Upload root directory
    :param str username: current user
    :param str task_id: mongo dentifier of the task

    :returns: The mongo identifier of the task
    """
    try:
        _delete(metax_client, fpath, root_upload_path, username, task_id)
    except Exception as error:
        logging.error(str(error), exc_info=error)
        tasks = db.Database().tasks
        tasks.update_status(task_id, "error")
        tasks.update_message(task_id, "Internal server error")
        raise

    return task_id


@FILES_API_V1.route("/<path:fpath>", methods=["POST"])
def upload_file(fpath):
    """ Save the uploaded file at <UPLOAD_PATH>/project/fpath

    :returns: HTTP Response
    """
    username = request.authorization.username
    database = db.Database()
    project = database.user(username).get_project()

    response = up.validate_upload()
    if response:
        return response

    file_path, file_name = utils.get_upload_path(project, fpath)

    # Create directory if it does not exist
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    file_path = os.path.join(file_path, file_name)
    try:
        response = up.save_file(project, file_path)
    except (up.OverwriteError) as error:
        return utils.make_response(409, str(error))

    database.user(request.authorization.username).update_used_quota(
        current_app.config.get("UPLOAD_PATH")
    )

    return response


@FILES_API_V1.route("/<path:fpath>", methods=["GET"])
def get_path(fpath):
    """Get filepath, name and checksum.

    :returns: HTTP Response
    """
    username = request.authorization.username
    database = db.Database()
    project = database.user(username).get_project()
    root_upload_path = current_app.config.get("UPLOAD_PATH")
    fpath, fname = utils.get_upload_path(project, fpath, root_upload_path)
    fpath = os.path.join(fpath, fname)

    if os.path.isfile(fpath):
        file_path = utils.get_return_path(project, fpath, root_upload_path)
        response = jsonify({
            "file_path": file_path,
            "metax_identifier": database.files.get_identifier(fpath),
            "md5": database.checksums.get_checksum(os.path.abspath(fpath)),
            "timestamp": md.iso8601_timestamp(fpath)
        })

    elif os.path.isdir(fpath):
        dir_tree = _get_dir_tree(project, fpath, root_upload_path)
        response = jsonify(dict(file_path=dir_tree))

    else:
        return utils.make_response(404, "File not found")

    response.status_code = 200
    return response


@FILES_API_V1.route("/<path:fpath>", methods=["DELETE"])
def delete_path(fpath):
    """Delete fpath under user's project. If fpath resolves to a directory,
    the whole directory is recursively removed.

    :returns: HTTP Response
    """
    root_upload_path = current_app.config.get("UPLOAD_PATH")
    username = request.authorization.username
    database = db.Database()
    project = database.user(username).get_project()
    fpath, fname = utils.get_upload_path(project, fpath)
    fpath = os.path.join(fpath, fname)

    if os.path.isfile(fpath):
        # Remove metadata from Metax
        try:
            response = md.MetaxClient().delete_file_metadata(project, fpath,
                                                             root_upload_path)
        except md.MetaxClientError as exception:
            response = str(exception)

        # Remove checksum from mongo
        database.checksums.delete_one(os.path.abspath(fpath))
        os.remove(fpath)

    elif os.path.isdir(fpath):
        # Remove all file metadata of files under dir fpath from Metax
        task_id = delete_task(md.MetaxClient(), fpath, root_upload_path,
                              username)

        polling_url = utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)
        response = jsonify({
            "file_path": fpath[len(os.path.join(root_upload_path, project)):],
            "message": "Deleting files and metadata",
            "polling_url": polling_url,
            "status": "pending"
        })
        location = url_for(TASK_STATUS_API_V1.name + ".task_status",
                           task_id=task_id)
        response.headers[b'Location'] = location
        response.status_code = 202
        return response

    else:
        return utils.make_response(404, "File not found")

    database.user(username).update_used_quota(root_upload_path)

    response = jsonify({
        "file_path": utils.get_return_path(project, fpath, root_upload_path),
        "message": "deleted",
        "metax": response
    })
    response.status_code = 200

    return response


@FILES_API_V1.route("", methods=["GET"], strict_slashes=False)
def get_files():
    """Get all files of the user

    :return: HTTP Response
    """
    username = request.authorization.username
    project = db.Database().user(username).get_project()
    root_upload_path = current_app.config.get("UPLOAD_PATH")
    fpath = safe_join(root_upload_path, secure_filename(project))

    if not os.path.exists(fpath):
        return utils.make_response(404, "No files found")

    response = jsonify(_get_dir_tree(project, fpath, root_upload_path))
    response.status_code = 200
    return response


@FILES_API_V1.route("", methods=["DELETE"], strict_slashes=False)
def delete_files():
    """Delete all files of a user

    :returns: HTTP Response
    """
    username = request.authorization.username
    project = db.Database().user(username).get_project()
    root_upload_path = current_app.config.get("UPLOAD_PATH")
    fpath = safe_join(root_upload_path, secure_filename(project))

    if not os.path.exists(fpath):
        return utils.make_response(404, "No files found")
    task_id = delete_task(md.MetaxClient(), fpath, root_upload_path, username)

    polling_url = utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)
    response = jsonify({
        "file_path": "/",
        "message": "Deleting files and metadata",
        "polling_url": polling_url,
        "status": "pending"
    })
    location = url_for(TASK_STATUS_API_V1.name + ".task_status",
                       task_id=task_id)
    response.headers[b'Location'] = location
    response.status_code = 202

    return response
