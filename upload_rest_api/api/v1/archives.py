"""/archives/v1 endpoints.

Functionality for uploading and extracting an archive.
"""
from archive_helpers.extract import (MemberNameError, MemberOverwriteError,
                                     MemberTypeError)
from flask import Blueprint, abort, jsonify, request

from upload_rest_api.upload import Upload
from upload_rest_api.utils import parse_relative_user_path

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

    upload = Upload(project_id, rel_upload_path)
    upload.validate(request.content_length, request.content_type)

    try:

        upload.save_stream(
            stream=request.stream,
            checksum=request.args.get('md5', None),
        )
        upload.validate_archive()
        polling_url = upload.store(file_type='archive')
    except (MemberOverwriteError) as error:
        abort(409, str(error))
    except MemberTypeError as error:
        abort(415, str(error))
    except MemberNameError as error:
        abort(400, str(error))

    response = jsonify(
        {
            "file_path": f"/{rel_upload_path}",
            "message": "Uploading archive",
            "polling_url": polling_url,
            "status": "pending"
        }
    )
    response.headers[b'Location'] = polling_url
    response.status_code = 202

    return response
