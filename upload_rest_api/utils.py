"""upload-rest-api utility functions"""
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


def make_response(status_code, message):
    """Returns jsonified default error message"""
    response = jsonify({"code": status_code, "error": message})
    response.status_code = status_code
    return response
