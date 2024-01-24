"""/archives/v1 endpoints.

Functionality for uploading and extracting an archive.
"""
from flask import Blueprint, jsonify, request, abort
import werkzeug

from upload_rest_api.authentication import current_user
from upload_rest_api.models.upload import Upload
from upload_rest_api.models.resource import Directory
from upload_rest_api.api.v1.tasks import get_polling_url
from upload_rest_api.jobs.utils import enqueue_background_job, UPLOAD_QUEUE
from upload_rest_api.lock import ProjectLockManager

ARCHIVES_API_V1 = Blueprint("archives_v1", __name__, url_prefix="/v1/archives")


@ARCHIVES_API_V1.route(
    "/<string:project_id>", methods=["POST"], strict_slashes=False
)
def upload_archive(project_id):
    """Upload and extract the archive at <UPLOAD_PROJECTS_PATH>/project.

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

    directory = Directory(project_id, request.args.get('dir', default='/'))
    upload = Upload.create(directory, size=request.content_length)
    checksum = request.args.get("md5", None)
    upload.add_source(file=request.stream, checksum=checksum)
    try:
        task_id = enqueue_background_job(
            task_func="upload_rest_api.jobs.upload.store_files",
            task_id=upload.id,
            queue_name=UPLOAD_QUEUE,
            project_id=upload.project.id,
            job_kwargs={
                "identifier": upload.id,
                "verify_source": bool(checksum)
            }
        )
    except Exception:
        # If we couldn't enqueue background job, release the lock
        lock_manager = ProjectLockManager()
        lock_manager.release(upload.project.id, upload.storage_path)
        raise

    response = jsonify(
        {
            "file_path": str(upload.path),
            "message": "Uploading archive",
            "polling_url": get_polling_url(task_id),
            "status": "pending"
        }
    )
    response.headers['Location'] = get_polling_url(task_id)
    response.status_code = 202

    return response
