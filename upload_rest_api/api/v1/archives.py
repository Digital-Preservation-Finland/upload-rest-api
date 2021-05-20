"""/archives/v1 endpoints.

Functionality for uploading and extracting an archive.
"""
import os

from flask import Blueprint, safe_join, request

from archive_helpers.extract import (
    MemberNameError, MemberOverwriteError, MemberTypeError
)

import upload_rest_api.upload as up
import upload_rest_api.database as db
import upload_rest_api.utils as utils

ARCHIVES_API_V1 = Blueprint("archives_v1", __name__, url_prefix="/v1/archives")


@ARCHIVES_API_V1.route("/", methods=["POST"], strict_slashes=False)
def upload_archive():
    """Upload and extract the archive at <UPLOAD_PATH>/project.

    :returns: HTTP Response
    """
    database = db.Database()
    response = up.validate_upload(database)
    if response:
        return response

    upload_dir = request.args.get("dir", default=None)
    file_path, file_name = utils.get_tmp_upload_path()

    # Create directory if it does not exist
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    file_path = safe_join(file_path, file_name)
    try:
        response = up.save_archive(database, file_path, upload_dir)
    except (MemberOverwriteError, up.OverwriteError) as error:
        response = utils.make_response(409, str(error))
    except MemberTypeError as error:
        response = utils.make_response(415, str(error))
    except MemberNameError as error:
        response = utils.make_response(400, str(error))
    except up.QuotaError as error:
        response = utils.make_response(413, str(error))
    except up.DataIntegrityError as error:
        response = utils.make_response(400, str(error))

    return response
