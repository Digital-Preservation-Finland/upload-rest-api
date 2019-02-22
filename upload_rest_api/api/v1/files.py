"""/filestorage/api/files/v1 endpoints. Functionality for uploading,
querying and deleting files from the server.
"""
import os
from shutil import rmtree

from flask import Blueprint, safe_join, request, jsonify, current_app
from werkzeug.utils import secure_filename

import upload_rest_api.upload as up
import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
import upload_rest_api.utils as utils


FILES_API_V1 = Blueprint("files_v1", __name__, url_prefix="/files/v1")


@FILES_API_V1.route("/<path:fpath>", methods=["POST"])
def upload_file(fpath):
    """Save the uploaded file at /var/spool/uploads/project/fpath

    :returns: HTTP Response
    """
    # Update used_quota also at the start of the function
    # since multiple users might by using the same project
    db.update_used_quota()

    if request.content_length > current_app.config.get("MAX_CONTENT_LENGTH"):
        return utils.make_response(413, "Max single file size exceeded")
    elif up.request_exceeds_quota():
        return utils.make_response(413, "Quota exceeded")

    fpath, fname = utils.get_upload_path(fpath)

    # Create directory if it does not exist
    if not os.path.exists(fpath):
        os.makedirs(fpath, 0o700)

    fpath = safe_join(fpath, fname)

    try:
        response = up.save_file(fpath, current_app.config.get("UPLOAD_PATH"))
    except up.OverwriteError as error:
        return utils.make_response(409, str(error))
    except up.SymlinkError as error:
        return utils.make_response(419, str(error))
    except up.QuotaError as error:
        return utils.make_response(413, str(error))

    db.update_used_quota()

    return response


@FILES_API_V1.route("/<path:fpath>", methods=["GET"])
def get_file(fpath):
    """Get filepath, name and checksum.

    :returns: HTTP Response
    """
    fpath, fname = utils.get_upload_path(fpath)
    fpath = safe_join(fpath, fname)

    if not os.path.isfile(fpath):
        return utils.make_response(404, "File not found")

    return jsonify({
        "file_path": utils.get_return_path(fpath),
        "metax_identifier": db.FilesCol().get_identifier(fpath),
        "md5": md.md5_digest(fpath),
        "timestamp": md.iso8601_timestamp(fpath)
    })


@FILES_API_V1.route("/<path:fpath>", methods=["DELETE"])
def delete_file(fpath):
    """Get filepath, name and checksum.

    :returns: HTTP Response
    """
    username = request.authorization.username
    project = db.UsersDoc(username).get_project()
    fpath, fname = utils.get_upload_path(fpath)
    fpath = safe_join(fpath, fname)

    if os.path.isfile(fpath):
        os.remove(fpath)
        db.update_used_quota()
    else:
        return utils.make_response(404, "File not found")

    # Remove metadata from Metax
    metax_response = md.MetaxClient().delete_file_metadata(project, fpath)

    return jsonify({
        "file_path": utils.get_return_path(fpath),
        "status": "deleted",
        "metax": metax_response
    })


@FILES_API_V1.route("", methods=["GET"])
def get_files():
    """Get all files of the user

    :return: HTTP Response
    """
    username = request.authorization.username
    project = db.UsersDoc(username).get_project()
    upload_path = current_app.config.get("UPLOAD_PATH")
    fpath = safe_join(upload_path, secure_filename(project))

    if not os.path.exists(fpath):
        return utils.make_response(404, "No files found")

    file_dict = {}
    for dirpath, _, files in os.walk(fpath):
        file_dict[utils.get_return_path(dirpath)] = files

    if "." in file_dict:
        file_dict["/"] = file_dict.pop(".")

    response = jsonify(file_dict)
    response.status_code = 200

    return response


@FILES_API_V1.route("", methods=["DELETE"])
def delete_files():
    """Delete all files of a user

    :returns: HTTP Response
    """
    username = request.authorization.username
    project = db.UsersDoc(username).get_project()
    upload_path = current_app.config.get("UPLOAD_PATH")
    fpath = safe_join(upload_path, secure_filename(project))

    if not os.path.exists(fpath):
        return utils.make_response(404, "No files found")

    # Remove metadata from Metax
    metax_response = md.MetaxClient().delete_all_metadata(project, fpath)

    # Remove project directory and update used_quota
    rmtree(fpath)
    db.update_used_quota()

    response = jsonify({
        "fpath": fpath[len(upload_path):],
        "status": "deleted",
        "metax": metax_response
    })
    response.status_code = 200

    return response
