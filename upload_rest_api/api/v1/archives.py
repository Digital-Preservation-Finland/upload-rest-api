"""/archives/v1 endpoints. Functionality for uploading and extracting an
archive.
"""
from __future__ import unicode_literals

import os

from flask import Blueprint, safe_join, request, current_app

from archive_helpers.extract import (
    MemberNameError, MemberOverwriteError, MemberTypeError
)

import upload_rest_api.upload as up
import upload_rest_api.database as db
import upload_rest_api.utils as utils

ARCHIVES_API_V1 = Blueprint("archives_v1", __name__, url_prefix="/v1/archives")


@ARCHIVES_API_V1.route("/", methods=["POST"], strict_slashes=False)
def upload_archive():
    """ Uploads and extracts the archive at <UPLOAD_PATH>/project

    :returns: HTTP Response
    """
    response = up.validate_upload()
    if response:
        return response

    upload_dir = request.args.get("dir", default=None)
    file_path, file_name = utils.get_tmp_upload_path()

    # Create directory if it does not exist
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    file_path = safe_join(file_path, file_name)
    try:
        response = up.save_archive(file_path, upload_dir)
    except (MemberOverwriteError, up.OverwriteError) as error:
        return utils.make_response(409, str(error))
    except MemberTypeError as error:
        return utils.make_response(415, str(error))
    except MemberNameError as error:
        return utils.make_response(400, str(error))
    except up.QuotaError as error:
        return utils.make_response(413, str(error))

    db.User(request.authorization.username).update_used_quota(
        current_app.config.get("UPLOAD_PATH")
    )

    return response
