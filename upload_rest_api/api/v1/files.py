"""/files/v1 endpoints.

Functionality for uploading, querying and deleting files from the
server.
"""
import os

from flask import (Blueprint, current_app, jsonify, request, safe_join,
                   url_for, abort)
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
    database = db.Database()
    user = database.user(request.authorization.username)
    upload_path = safe_join("", fpath)

    up.validate_upload(user, request.content_length, request.content_type)

    md5 = up.save_file(database,
                       user,
                       request.stream,
                       request.args.get('md5', None),
                       upload_path)

    return jsonify(
        {
            'file_path': f"/{upload_path}",
            'md5': md5,
            'status': 'created'
        }
    )


@FILES_API_V1.route("/", defaults={'fpath': ""}, methods=["GET"])
@FILES_API_V1.route("/<path:fpath>", methods=["GET"])
def get_path(fpath):
    """Get filepath, name and checksum.

    :returns: HTTP Response
    """
    username = request.authorization.username
    database = db.Database()
    user = database.user(username)

    upload_path = utils.get_upload_path(user, fpath)
    return_path = utils.get_return_path(user, upload_path)

    if os.path.isfile(upload_path):
        response = {
            "file_path": return_path,
            "identifier": database.files.get_identifier(upload_path),
            "md5": database.checksums.get_checksum(
                os.path.abspath(upload_path)
            ),
            "timestamp": md.iso8601_timestamp(upload_path)
        }

    elif os.path.isdir(upload_path):
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
        entries = list(os.scandir(upload_path))

        response = {
            'identifier': identifier,
            'directories': [entry.name for entry in entries if entry.is_dir()],
            'files':  [entry.name for entry in entries if entry.is_file()]
        }

    else:
        abort(404, "File not found")

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
    upload_path = utils.get_upload_path(user, fpath)

    if os.path.isfile(upload_path):
        # Remove metadata from Metax
        try:
            response = md.MetaxClient().delete_file_metadata(
                user.get_project(), upload_path, root_upload_path
            )
        except md.MetaxClientError as exception:
            response = str(exception)

        # Remove checksum from mongo
        database.checksums.delete_one(os.path.abspath(upload_path))
        os.remove(upload_path)

    elif upload_path.exists() \
            and upload_path.samefile(user.project_directory) \
            and not any(upload_path.iterdir()):
        # Trying to delete empty project directory
        abort(404, "No files found")

    elif os.path.isdir(upload_path):
        # Remove all file metadata of files under fpath from Metax
        task_id = enqueue_background_job(
            task_func="upload_rest_api.jobs.files.delete_files",
            queue_name=FILES_QUEUE,
            username=username,
            job_kwargs={
                "fpath": upload_path,
                "username": username
            }
        )

        polling_url = utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)
        response = jsonify({
            "file_path": utils.get_return_path(user, upload_path),
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
        abort(404, "File not found")

    database.user(username).update_used_quota(root_upload_path)

    response = jsonify({
        "file_path": utils.get_return_path(user, upload_path),
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
        abort(404, "No files found")

    response = jsonify(_get_dir_tree(user, fpath))
    response.status_code = 200
    return response
