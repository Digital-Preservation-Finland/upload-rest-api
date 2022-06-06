"""REST api for uploading files into passipservice."""
import os.path

from flask import Blueprint, abort, jsonify, url_for

from upload_rest_api import utils
from upload_rest_api.api.v1.tasks import TASK_STATUS_API_V1
from upload_rest_api.authentication import current_user
from upload_rest_api.database import Projects
from upload_rest_api.jobs.utils import METADATA_QUEUE, enqueue_background_job
from upload_rest_api.lock import lock_manager

METADATA_API_V1 = Blueprint("metadata_v1", __name__, url_prefix="/v1/metadata")


@METADATA_API_V1.route("/<string:project_id>/<path:fpath>", methods=["POST"])
def post_metadata(project_id, fpath):
    """POST file metadata to Metax.

    A background task is launched to run the job. The ``Location``
    header and the body of the response contain the URL to be used for
    polling the status of the task. Status code is set to HTTP
    202(Accepted).

    :returns: HTTP Response
    """
    if not current_user.is_allowed_to_access_project(project_id):
        abort(403)

    file_path = Projects.get_upload_path(project_id, fpath)

    lock_manager.acquire(project_id, file_path)

    try:
        if not os.path.exists(file_path):
            abort(404, "File not found")

        task_id = enqueue_background_job(
            task_func="upload_rest_api.jobs.metadata.post_metadata",
            queue_name=METADATA_QUEUE,
            project_id=project_id,
            job_kwargs={
                "path": fpath,
                "project_id": project_id
            }
        )
    except Exception:
        # Ensure we don't hold the lock if we are unable to enqueue
        # the background job
        lock_manager.release(project_id, file_path)
        raise

    polling_url = utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)
    ret_path = Projects.get_return_path(project_id, file_path)
    response = jsonify({
        "file_path": ret_path,
        "message": "Creating metadata",
        "polling_url": polling_url,
        "status": "pending"
    })
    location = url_for(TASK_STATUS_API_V1.name + ".task_status",
                       task_id=task_id)
    response.headers[b'Location'] = location
    response.status_code = 202

    return response


@METADATA_API_V1.route("/<string:project_id>/<path:fpath>", methods=["DELETE"])
def delete_metadata(project_id, fpath):
    """Delete fpath metadata under project.

    A background task is launched to run the job. If fpath resolves to a
    directory metadata is recursively removed all the files under the
    directory. The ``Location`` header and the body of the response
    contain the URL to be used for polling the status of the task.
    Status code is set to HTTP 202(Accepted).

    :returns: HTTP Response
    """
    if not current_user.is_allowed_to_access_project(project_id):
        abort(403)

    file_path = Projects.get_upload_path(project_id, fpath)

    lock_manager.acquire(project_id, file_path)
    try:
        task_id = enqueue_background_job(
            task_func="upload_rest_api.jobs.metadata.delete_metadata",
            queue_name=METADATA_QUEUE,
            project_id=project_id,
            job_kwargs={
                "fpath": fpath,
                "project_id": project_id
            }
        )
    except Exception:
        lock_manager.release(project_id, file_path)
        raise

    polling_url = utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)
    ret_path = Projects.get_return_path(project_id, file_path)
    response = jsonify({
        "file_path": ret_path,
        "message": "Deleting metadata",
        "polling_url": polling_url,
        "status": "pending"
    })
    location = url_for(TASK_STATUS_API_V1.name + ".task_status",
                       task_id=task_id)
    response.headers[b'Location'] = location
    response.status_code = 202

    return response
