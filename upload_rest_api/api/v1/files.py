"""/files/v1 endpoints.

Functionality for uploading, querying and deleting files from the
server.
"""
import os
import pathlib

from flask import Blueprint, abort, jsonify, request
import werkzeug

from upload_rest_api.api.v1.tasks import get_polling_url
from upload_rest_api.authentication import current_user
from upload_rest_api.resource import get_resource
from upload_rest_api.upload import create_upload

FILES_API_V1 = Blueprint("files_v1", __name__, url_prefix="/v1/files")


def _get_dir_tree(project):
    """Return with dir tree from project directory."""
    file_dict = {}
    for dirpath, _, files in os.walk(project.directory):
        path = pathlib.Path(dirpath).relative_to(project.directory)
        file_dict[f'/{path}'] = files

    if "/." in file_dict:
        file_dict["/"] = file_dict.pop("/.")

    return file_dict


@FILES_API_V1.route("/<string:project_id>/<path:fpath>", methods=["POST"])
def upload_file(project_id, fpath):
    """Save the uploaded file at <UPLOAD_PROJECTS_PATH>/project/fpath.

    :returns: HTTP Response
    """
    if not current_user.is_allowed_to_access_project(project_id):
        abort(403, "No permission to access this project")

    if request.content_type not in ('application/octet-stream', None):
        raise werkzeug.exceptions.UnsupportedMediaType(
            f"Unsupported Content-Type: {request.content_type}"
        )

    if request.content_length is None:
        raise werkzeug.exceptions.LengthRequired(
            "Missing Content-Length header"
        )

    upload = create_upload(project_id, fpath, request.content_length)
    upload.add_source(request.stream, request.args.get('md5', None))
    upload.store_files()

    return jsonify(
        {
            'file_path': str(upload.path),
            'status': 'created'
        }
    )


@FILES_API_V1.route(
    "/<string:project_id>/", defaults={'fpath': ""}, methods=["GET"],
    strict_slashes=False
)
@FILES_API_V1.route(
    "/<string:project_id>/<path:fpath>", methods=["GET"]
)
def get_path(project_id, fpath):
    """Get filepath, name and checksum.

    :returns: HTTP Response
    """
    if not current_user.is_allowed_to_access_project(project_id):
        abort(403, "No permission to access this project")

    try:
        resource = get_resource(project_id, fpath)
    except FileNotFoundError:
        abort(404, "File not found")

    if request.args.get("all", None) == "true" and fpath.strip("/") == "":
        return jsonify(_get_dir_tree(resource.project))

    if resource.storage_path.is_file():
        response = {
            "file_path": str(resource.path),
            "identifier": resource.identifier,
            "md5": resource.checksum,
            "timestamp": resource.timestamp
        }

    elif resource.storage_path.is_dir():
        response = {
            'identifier': resource.identifier,
            'directories': [dir.path.name for
                            dir in resource.get_directories()],
            'files':  [file.path.name for file in resource.get_files()]
        }

    return response


@FILES_API_V1.route(
    "/<string:project_id>", defaults={'fpath': ""}, methods=["DELETE"],
    strict_slashes=False
)
@FILES_API_V1.route("/<string:project_id>/<path:fpath>", methods=["DELETE"])
def delete_path(project_id, fpath):
    """Delete fpath under project.

    If fpath resolves to a directory, the whole directory is recursively
    removed.

    :returns: HTTP Response
    """
    if not current_user.is_allowed_to_access_project(project_id):
        abort(403, "No permission to access this project")

    try:
        resource = get_resource(project_id, fpath)
    except FileNotFoundError:
        abort(404, "File not found")

    if resource.has_pending_dataset():
        abort(
            400,
            "File/directory is used in a pending dataset and cannot be deleted"
        )

    if resource.storage_path.is_file():
        metax_response = resource.delete()

        response = jsonify({
            "file_path": str(resource.path),
            "message": "deleted",
            "metax": metax_response
        })
        response.status_code = 200

    elif resource.storage_path.is_dir():
        try:
            task_id = resource.delete()
        except FileNotFoundError:
            # Trying to delete empty project directory
            abort(404, "No files found")

        polling_url = get_polling_url(task_id)
        response = jsonify({
            "file_path": str(resource.path),
            "message": "Deleting metadata",
            "polling_url": polling_url,
            "status": "pending"
        })
        response.headers[b'Location'] = polling_url
        response.status_code = 202

    return response
