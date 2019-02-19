"""REST api for uploading files into passipservice
"""
import os

from flask import Blueprint, current_app, abort, safe_join, request, jsonify
from werkzeug.utils import secure_filename

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md


METADATA_API_V1 = Blueprint("metadata_v1", __name__, url_prefix="/metadata/v1")


def _get_upload_path(fpath):
    """Get upload path for current request"""
    username = request.authorization.username
    user = db.UsersDoc(username)
    project = user.get_project()

    upload_path = current_app.config.get("UPLOAD_PATH")
    fpath, fname = os.path.split(fpath)
    fname = secure_filename(fname)
    project = secure_filename(project)

    joined_path = safe_join(upload_path, project)
    joined_path = safe_join(joined_path, fpath)

    return joined_path, fname


@METADATA_API_V1.route("/<path:fpath>", methods=["POST"])
def post_metadata(fpath):
    """POST file metadata to Metax

    :returns: HTTP Response
    """
    fpath, fname = _get_upload_path(fpath)
    fpath = safe_join(fpath, fname)

    if os.path.isdir(fpath):
        # POST metadata of all files under dir fpath
        fpaths = []
        for dirpath, _, files in os.walk(fpath):
            for fname in files:
                fpaths.append(os.path.join(dirpath, fname))

    elif os.path.isfile(fpath):
        fpaths = [fpath]

    else:
        abort(404, "File not found")

    metax_client = md.MetaxClient()
    response = metax_client.post_metadata(fpaths)

    # Add created identifiers to Mongo
    if "success" in response and len(response["success"]) > 0:
        created_md = response["success"]
        db.FilesCol().store_identifiers(created_md)

    return jsonify(response)
