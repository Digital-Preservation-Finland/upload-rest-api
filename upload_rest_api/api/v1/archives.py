"""/archives/v1 endpoints.

Functionality for uploading and extracting an archive.
"""
from flask import Blueprint, safe_join, request, jsonify, abort

from archive_helpers.extract import (
    MemberNameError, MemberOverwriteError, MemberTypeError
)

import upload_rest_api.upload as up
import upload_rest_api.database as db

ARCHIVES_API_V1 = Blueprint("archives_v1", __name__, url_prefix="/v1/archives")


@ARCHIVES_API_V1.route("/<string:project_id>", methods=["POST"], strict_slashes=False)
def upload_archive(project_id):
    """Upload and extract the archive at <UPLOAD_PATH>/project.

    :returns: HTTP Response
    """
    database = db.Database()
    up.validate_upload(project_id, request.content_length, request.content_type)

    upload_path = safe_join("",
                            request.args.get('dir', default='').lstrip('/'))

    try:
        polling_url = up.save_archive(
            database=database,
            project_id=project_id,
            stream=request.stream,
            checksum=request.args.get('md5', None),
            upload_path=upload_path
        )
    except (MemberOverwriteError) as error:
        abort(409, str(error))
    except MemberTypeError as error:
        abort(415, str(error))
    except MemberNameError as error:
        abort(400, str(error))

    response = jsonify(
        {
            "file_path": f"/{upload_path}",
            "message": "Uploading archive",
            "polling_url": polling_url,
            "status": "pending"
        }
    )
    response.headers[b'Location'] = polling_url
    response.status_code = 202

    return response
