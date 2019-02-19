"""REST api for uploading files into passipservice
"""
import os

from flask import Blueprint, safe_join, jsonify

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
import upload_rest_api.utils as utils


METADATA_API_V1 = Blueprint("metadata_v1", __name__, url_prefix="/metadata/v1")


@METADATA_API_V1.route("/<path:fpath>", methods=["POST"])
def post_metadata(fpath):
    """POST file metadata to Metax

    :returns: HTTP Response
    """
    fpath, fname = utils.get_upload_path(fpath)
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
        return utils.make_response(404, "File not found")

    metax_client = md.MetaxClient()
    response = metax_client.post_metadata(fpaths)

    # Add created identifiers to Mongo
    if "success" in response and len(response["success"]) > 0:
        created_md = response["success"]
        db.FilesCol().store_identifiers(created_md)

    return jsonify(response)
