"""/files/v1 endpoints.

Functionality for uploading, querying and deleting files from the
server.
"""
import os
from pathlib import Path

import werkzeug
from flask import Blueprint, abort, jsonify, request

from upload_rest_api.api.v1.tasks import get_polling_url
from upload_rest_api.authentication import current_user
from upload_rest_api.models.upload import Upload
from upload_rest_api.models.resource import get_resource, File
from upload_rest_api.config import CONFIG
from upload_rest_api.jobs import FILES_QUEUE, enqueue_background_job
from upload_rest_api.lock import ProjectLockManager

FILES_API_V1 = Blueprint("files_v1", __name__, url_prefix="/v1/files")


def _get_dir_tree(project):
    """Return with dir tree from project directory."""
    file_dict = {}
    for dirpath, _, files in os.walk(project.directory):

        path = Path(dirpath).relative_to(project.directory)
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

    if request.content_type not in ('application/octet-stream', None, ''):
        raise werkzeug.exceptions.UnsupportedMediaType(
            f"Unsupported Content-Type: {request.content_type}"
        )

    if request.content_length is None:
        raise werkzeug.exceptions.LengthRequired(
            "Missing Content-Length header"
        )

    # Upload file
    file = File(project_id, fpath)
    upload = Upload.create(file, request.content_length)
    checksum = request.args.get('md5', None)
    upload.add_source(request.stream, checksum)
    upload.store_files(verify_source=bool(checksum))

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
            'directories': [dir_.path.name for
                            dir_ in resource.get_directories()],
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

        is_project_dir \
            = resource.storage_path.samefile(resource.project.directory)
        if is_project_dir and not any(resource.project.directory.iterdir()):
            # Trying to delete empty project directory
            abort(404, "No files found")

        # Acquire a lock *and* keep it alive even after this HTTP
        # request. It will be released by the 'delete_directory'
        # background job once it finishes.
        lock_manager = ProjectLockManager()
        lock_manager.acquire(resource.project.id, resource.storage_path)

        try:
            task_id = enqueue_background_job(
                task_func="upload_rest_api.jobs.files.delete_directory",
                queue_name=FILES_QUEUE,
                project_id=resource.project.id,
                job_kwargs={
                    "project_id": resource.project.id,
                    "path": str(resource.path),
                }
            )
        except Exception:
            # If we couldn't enqueue background job, release the lock
            lock_manager.release(resource.project.id, resource.storage_path)
            raise

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


@FILES_API_V1.route("/get_size_limit", methods=["GET"])
def get_file_size_limit():
    return jsonify({"file_size_limit": CONFIG["MAX_CONTENT_LENGTH"]})
