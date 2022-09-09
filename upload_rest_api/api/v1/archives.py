"""/archives/v1 endpoints.

Functionality for uploading and extracting an archive.
"""
from flask import Blueprint, abort, jsonify, request
import werkzeug

from upload_rest_api.upload import create_upload
from upload_rest_api.utils import parse_relative_user_path
from upload_rest_api.api.v1.tasks import get_polling_url

ARCHIVES_API_V1 = Blueprint("archives_v1", __name__, url_prefix="/v1/archives")


@ARCHIVES_API_V1.route(
    "/<string:project_id>", methods=["POST"], strict_slashes=False
)
def upload_archive(project_id):
    """Upload and extract the archive at <UPLOAD_PROJECTS_PATH>/project.

    :returns: HTTP Response
    """
    try:
        rel_upload_path = parse_relative_user_path(
            request.args.get('dir', default='').lstrip('/')
        )
    except ValueError:
        abort(404)

    if request.content_type not in ('application/octet-stream', None):
        raise werkzeug.exceptions.UnsupportedMediaType(
            f"Unsupported Content-Type: {request.content_type}"
        )

    if request.content_length is None:
        raise werkzeug.exceptions.LengthRequired(
            "Missing Content-Length header"
        )

    upload = create_upload(project_id, rel_upload_path,
                           size=request.content_length,
                           upload_type='archive')
    upload.add_source(file=request.stream,
                      checksum=request.args.get('md5', None))
    upload.validate_archive()
    task_id = upload.enqueue_store_task()

    response = jsonify(
        {
            "file_path": f"/{rel_upload_path}",
            "message": "Uploading archive",
            "polling_url": get_polling_url(task_id),
            "status": "pending"
        }
    )
    response.headers[b'Location'] = get_polling_url(task_id)
    response.status_code = 202

    return response
