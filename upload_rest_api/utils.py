"""upload-rest-api utility functions"""
from __future__ import unicode_literals

import os

from flask import request, current_app, jsonify, safe_join
from werkzeug.utils import secure_filename

import upload_rest_api.database as db


def get_upload_path(fpath):
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


def get_return_path(fpath):
    """Splice upload_path and project from fpath and return the path
    shown to the user and POSTed to Metax.
    """
    username = request.authorization.username
    user = db.UsersDoc(username)
    project = user.get_project()
    upload_path = current_app.config.get("UPLOAD_PATH")
    base_path = safe_join(upload_path, project)

    return os.path.normpath(fpath[len(base_path):])


def make_response(status_code, message):
    """Returns jsonified default error message"""
    response = jsonify({"code": status_code, "error": message})
    response.status_code = status_code
    return response
