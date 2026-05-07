"""/files/v1 endpoints.

Functionality for uploading, querying and deleting files from the
server.
"""
from flask import Blueprint, abort, jsonify

from upload_rest_api.api.v1.tasks import get_polling_url
from upload_rest_api.authentication import require_project_access
from upload_rest_api.config import CONFIG
from upload_rest_api.jobs import FILES_QUEUE, enqueue_background_job
from upload_rest_api.lock import ProjectLockManager
from upload_rest_api.models.resource import get_resource

FILES_API_V1 = Blueprint("files_v1", __name__, url_prefix="/v1/files")


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
    require_project_access(project_id)

    try:
        resource = get_resource(project_id, fpath)
    except FileNotFoundError:
        abort(404, "File not found")

    if resource.storage_path.is_file():
        response = {
            "file_path": str(resource.path),
            "identifier": resource.identifier,
            "md5": resource.checksum,
            "timestamp": resource.timestamp
        }
    elif resource.storage_path.is_dir():
        response = {
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
    require_project_access(project_id)

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
        response.headers['Location'] = polling_url
        response.status_code = 202

    return response


@FILES_API_V1.route("/get_size_limit", methods=["GET"])
def get_file_size_limit():
    return jsonify({"file_size_limit": CONFIG["MAX_CONTENT_LENGTH"]})
