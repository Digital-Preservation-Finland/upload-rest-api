"""REST api for uploading files into passipservice
"""
from __future__ import unicode_literals

import os

from flask import Blueprint, safe_join, jsonify, request
from requests.exceptions import HTTPError

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
import upload_rest_api.utils as utils


METADATA_API_V1 = Blueprint("metadata_v1", __name__, url_prefix="/v1/metadata")


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

    status_code = 200
    metax_client = md.MetaxClient()
    try:
        response = metax_client.post_metadata(fpaths)
    except HTTPError as exception:
        response = exception.response.json()
        status_code = exception.response.status_code

    # Add created identifiers to Mongo
    if "success" in response and response["success"]:
        created_md = response["success"]
        db.FilesCol().store_identifiers(created_md)

    # Create upload-rest-api response
    upload_response = {"code": status_code, "metax_response": response}
    upload_response = jsonify(upload_response)
    upload_response.status_code = status_code

    return upload_response


@METADATA_API_V1.route("/<path:fpath>", methods=["DELETE"])
def delete_metadata(fpath):
    """Delete fpath metadata under user's project. If fpath resolves to a
    directory metadata is recursively removed all the files under the
    directory.

    :returns: HTTP Response
    """
    username = request.authorization.username
    project = db.UsersDoc(username).get_project()
    fpath, fname = utils.get_upload_path(fpath)
    fpath = safe_join(fpath, fname)
    client = md.MetaxClient()

    if os.path.isfile(fpath):
        # Remove metadata from Metax
        delete_func = client.delete_file_metadata
    elif os.path.isdir(fpath):
        # Remove all file metadata of files under dir fpath from Metax
        delete_func = client.delete_all_metadata
    else:
        return utils.make_response(404, "File not found")

    status_code = 200
    try:
        response = delete_func(project, fpath, force=True)
    except HTTPError as exception:
        response = exception.response.json()
        status_code = exception.response.status_code
    except md.MetaxClientError as exception:
        return utils.make_response(400, str(exception))

    response = jsonify({
        "file_path": utils.get_return_path(fpath),
        "metax": response
    })
    response.status_code = status_code

    return response
