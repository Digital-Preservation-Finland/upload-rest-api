"""/files/v1 endpoints.

Functionality for uploading, querying and deleting files from the
server.
"""
import os

from flask import Blueprint, current_app, jsonify, request, url_for
import metax_access

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
import upload_rest_api.upload as up
import upload_rest_api.utils as utils
from upload_rest_api.api.v1.tasks import TASK_STATUS_API_V1
from upload_rest_api.jobs.utils import FILES_QUEUE, enqueue_background_job

FILES_API_V1 = Blueprint("files_v1", __name__, url_prefix="/v1/files")


def _get_dir_tree(user, fpath):
    """Return with dir tree from fpath as a dict."""
    file_dict = {}
    for dirpath, _, files in os.walk(fpath):
        path = utils.get_return_path(user, dirpath)
        file_dict[path] = files

    if "." in file_dict:
        file_dict["/"] = file_dict.pop(".")

    return file_dict


@FILES_API_V1.route("/<path:fpath>", methods=["POST"])
def upload_file(fpath):
    """Save the uploaded file at <UPLOAD_PATH>/project/fpath.

    :returns: HTTP Response
    """
    username = request.authorization.username
    database = db.Database()
    user = database.user(username)

    response = up.validate_upload(database)
    if response:
        return response

    file_path, file_name = utils.get_upload_path(user, fpath)

    # Create directory if it does not exist
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    file_path = os.path.join(file_path, file_name)
    try:
        response = up.save_file(database, user, file_path)
    except up.OverwriteError as error:
        response = utils.make_response(409, str(error))
    except up.DataIntegrityError as error:
        response = utils.make_response(400, str(error))
    else:
        database.user(request.authorization.username).update_used_quota(
            current_app.config.get("UPLOAD_PATH")
        )

    return response


@FILES_API_V1.route("/", defaults={'fpath': ""}, methods=["GET"])
@FILES_API_V1.route("/<path:fpath>", methods=["GET"])
def get_path(fpath):
    """Get filepath, name and checksum.

    :returns: HTTP Response
    """
    username = request.authorization.username
    database = db.Database()
    user = database.user(username)

    fpath, fname = utils.get_upload_path(user, fpath)
    fpath = os.path.join(fpath, fname)
    return_path = utils.get_return_path(user, fpath)

    if os.path.isfile(fpath):
        response = {
            "file_path": return_path,
            "identifier": database.files.get_identifier(fpath),
            "md5": database.checksums.get_checksum(os.path.abspath(fpath)),
            "timestamp": md.iso8601_timestamp(fpath)
        }

    elif os.path.isdir(fpath):
        metax = metax_access.Metax(
            url=current_app.config.get("METAX_URL"),
            user=current_app.config.get("METAX_USER"),
            password=current_app.config.get("METAX_PASSWORD"),
            verify=current_app.config.get("METAX_SSL_VERIFICATION")
        )
        try:
            identifier = metax.get_project_directory(user.get_project(),
                                                     return_path)['identifier']
        except metax_access.DirectoryNotAvailableError:
            identifier = None

        # Create a list of directories and files to avoid scanning the
        # directory twice
        entries = list(os.scandir(fpath))

        response = {
            'identifier': identifier,
            'directories': [entry.name for entry in entries if entry.is_dir()],
            'files':  [entry.name for entry in entries if entry.is_file()]
        }

    else:
        return utils.make_response(404, "File not found")

    return response


@FILES_API_V1.route("/", defaults={'fpath': ""}, methods=["DELETE"],
                    strict_slashes=False)
@FILES_API_V1.route("/<path:fpath>", methods=["DELETE"])
def delete_path(fpath):
    """Delete fpath under user's project.

    If fpath resolves to a directory, the whole directory is recursively
    removed.

    :returns: HTTP Response
    """
    root_upload_path = current_app.config.get("UPLOAD_PATH")
    username = request.authorization.username
    database = db.Database()
    user = database.user(username)
    fpath, fname = utils.get_upload_path(user, fpath)
    fpath = os.path.join(fpath, fname)

    if os.path.isfile(fpath):
        # Remove metadata from Metax
        try:
            response = md.MetaxClient().delete_file_metadata(
                user.get_project(), fpath, root_upload_path
            )
        except md.MetaxClientError as exception:
            response = str(exception)

        # Remove checksum from mongo
        database.checksums.delete_one(os.path.abspath(fpath))
        os.remove(fpath)

    elif os.path.isdir(fpath):
        # Remove all file metadata of files under dir fpath from Metax
        task_id = enqueue_background_job(
            task_func="upload_rest_api.jobs.files.delete_files",
            queue_name=FILES_QUEUE,
            username=username,
            job_kwargs={
                "fpath": fpath,
                "username": username
            }
        )

        polling_url = utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)
        response = jsonify({
            "file_path": fpath[len(os.path.join(root_upload_path,
                                                user.get_project())):],
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
        "file_path": utils.get_return_path(user, fpath),
        "message": "deleted",
        "metax": response
    })
    response.status_code = 200

    return response


@FILES_API_V1.route("", methods=["GET"])
def get_files():
    """Get all files of the user.

    :return: HTTP Response
    """
    username = request.authorization.username
    user = db.Database().user(username)
    fpath = user.project_directory

    if not os.path.exists(fpath):
        return utils.make_response(404, "No files found")

    response = jsonify(_get_dir_tree(user, fpath))
    response.status_code = 200
    return response
