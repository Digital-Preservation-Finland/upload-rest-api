"""/files/v1 endpoints. Functionality for uploading,
querying and deleting files from the server.
"""
from __future__ import unicode_literals

import os
from shutil import rmtree
import json

from flask import (Blueprint, safe_join, request, jsonify,
                   current_app)
from werkzeug.utils import secure_filename
from requests.exceptions import HTTPError

from metax_access import MetaxError

from archive_helpers.extract import (
    MemberNameError, MemberOverwriteError, MemberTypeError
)

import upload_rest_api.upload as up
import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
import upload_rest_api.utils as utils
from upload_rest_api.api.v1.tasks import TASK_STATUS_API_V1

FILES_API_V1 = Blueprint("files_v1", __name__, url_prefix="/v1/files")
ARCHIVES_API_V1 = Blueprint("archives_v1", __name__, url_prefix="/v1/archives")
SUPPORTED_TYPES = ("application/octet-stream",)


def _get_dir_tree(fpath):
    """Returns with dir tree from fpath as a dict"""
    file_dict = {}
    for dirpath, _, files in os.walk(fpath):
        path = utils.get_return_path(dirpath)
        file_dict[path] = files

    if "." in file_dict:
        file_dict["/"] = file_dict.pop(".")

    return file_dict


def _validate_upload():
    """Validates the upload request

    :returns: `None` if the validation succeeds. Otherwise error response
        if validation failed.
    """
    response = None
    # Update used_quota also at the start of the function
    # since multiple users might by using the same project
    db.update_used_quota(request.authorization.username,
                         current_app.config.get("UPLOAD_PATH"))

    # Check that Content-Length header is provided
    if request.content_length is None:
        response = utils.make_response(400, "Missing Content-Length header")

    # Check that Content-Type is supported if the header is provided
    content_type = request.content_type
    if content_type and content_type not in SUPPORTED_TYPES:
        response = utils.make_response(
            415, "Unsupported Content-Type: %s" % content_type
        )

    # Check user quota
    if request.content_length > current_app.config.get("MAX_CONTENT_LENGTH"):
        response = utils.make_response(413, "Max single file size exceeded")
    elif up.request_exceeds_quota():
        response = utils.make_response(413, "Quota exceeded")
    return response


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

    # Remove metadata from Metax
    ret_path = utils.get_return_path(fpath, root_upload_path, username)
    db.AsyncTaskCol().update_message(
        task_id,
        "Deleting files and metadata: %s" % ret_path
    )
    project = db.UsersDoc(username).get_project()
    try:
        metax_response = metax_client.delete_all_metadata(project, fpath,
                                                          root_upload_path)
    except (MetaxError, HTTPError) as exc:
        db.AsyncTaskCol().update_status(task_id, "error")
        msg = {"message": str(exc)}
        db.AsyncTaskCol().update_message(task_id, json.dumps(msg))
    else:
        # Remove checksum from mongo
        db.ChecksumsCol().delete_dir(fpath)

        # Remove project directory and update used_quota
        rmtree(fpath)
        db.update_used_quota(username, root_upload_path)
        db.AsyncTaskCol().update_status(task_id, "done")
        response = {
            "file_path": ret_path,
            "status": "done",
            "metax": metax_response
        }
        db.AsyncTaskCol().update_message(task_id, json.dumps(response))
    return task_id


@FILES_API_V1.route("/<path:fpath>", methods=["POST"])
def upload_file(fpath):
    """ Save the uploaded file at <UPLOAD_PATH>/project/fpath

    :returns: HTTP Response
    """
    response = _validate_upload()
    if response:
        return response

    file_path, file_name = utils.get_upload_path(fpath)

    # Create directory if it does not exist
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    file_path = safe_join(file_path, file_name)
    try:
        response = up.save_file(file_path)
    except (up.OverwriteError) as error:
        return utils.make_response(409, str(error))

    db.update_used_quota(request.authorization.username,
                         current_app.config.get("UPLOAD_PATH"))

    return response


@ARCHIVES_API_V1.route("/", methods=["POST"], strict_slashes=False)
def upload_archive():
    """ Uploads and extracts the archive at <UPLOAD_PATH>/project

    :returns: HTTP Response
    """
    response = _validate_upload()
    if response:
        return response

    file_path, file_name = utils.get_tmp_upload_path()

    # Create directory if it does not exist
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    file_path = safe_join(file_path, file_name)
    try:
        response = up.save_archive(file_path)
    except (MemberOverwriteError, up.OverwriteError) as error:
        return utils.make_response(409, str(error))
    except MemberTypeError as error:
        return utils.make_response(415, str(error))
    except MemberNameError as error:
        return utils.make_response(400, str(error))
    except up.QuotaError as error:
        return utils.make_response(413, str(error))

    db.update_used_quota(request.authorization.username,
                         current_app.config.get("UPLOAD_PATH"))

    return response


@FILES_API_V1.route("/<path:fpath>", methods=["GET"])
def get_path(fpath):
    """Get filepath, name and checksum.

    :returns: HTTP Response
    """
    username = request.authorization.username
    root_upload_path = current_app.config.get("UPLOAD_PATH")
    fpath, fname = utils.get_upload_path(fpath)
    fpath = safe_join(fpath, fname)

    if os.path.isfile(fpath):
        file_path = utils.get_return_path(fpath, root_upload_path, username)
        response = jsonify({
            "file_path": file_path,
            "metax_identifier": db.FilesCol().get_identifier(fpath),
            "md5": db.ChecksumsCol().get_checksum(os.path.abspath(fpath)),
            "timestamp": md.iso8601_timestamp(fpath)
        })

    elif os.path.isdir(fpath):
        dir_tree = _get_dir_tree(fpath)
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
    project = db.UsersDoc(username).get_project()
    fpath, fname = utils.get_upload_path(fpath)
    fpath = safe_join(fpath, fname)

    if os.path.isfile(fpath):
        # Remove metadata from Metax
        try:
            response = md.MetaxClient().delete_file_metadata(project, fpath,
                                                             root_upload_path)
        except md.MetaxClientError as exception:
            response = str(exception)

        # Remove checksum from mongo
        db.ChecksumsCol().delete_one(os.path.abspath(fpath))
        os.remove(fpath)

    elif os.path.isdir(fpath):
        # Remove all file metadata of files under dir fpath from Metax
        task_id = delete_task(md.MetaxClient(), fpath, root_upload_path,
                              username)

        polling_url = utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)
        response = jsonify({
            "file_path": fpath[len(root_upload_path):],
            "message": "Deleting files and metadata",
            "polling_url": polling_url,
            "status": "pending"
        })
        response.headers[b'Location'] = polling_url
        response.status_code = 202
        return response

    else:
        return utils.make_response(404, "File not found")

    db.update_used_quota(username, root_upload_path)

    response = jsonify({
        "file_path": utils.get_return_path(fpath, root_upload_path, username),
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
    project = db.UsersDoc(username).get_project()
    root_upload_path = current_app.config.get("UPLOAD_PATH")
    fpath = safe_join(root_upload_path, secure_filename(project))

    if not os.path.exists(fpath):
        return utils.make_response(404, "No files found")

    response = jsonify(_get_dir_tree(fpath))
    response.status_code = 200
    return response


@FILES_API_V1.route("", methods=["DELETE"], strict_slashes=False)
def delete_files():
    """Delete all files of a user

    :returns: HTTP Response
    """
    username = request.authorization.username
    project = db.UsersDoc(username).get_project()
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
    response.headers[b'Location'] = polling_url
    response.status_code = 202

    return response
