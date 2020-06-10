"""upload-rest-api utility functions"""
from __future__ import unicode_literals

import os
import uuid
from functools import wraps
try:
    from urllib.parse import urlparse, urlunparse
except ImportError:  # Python 2
    from urlparse import urlparse, urlunparse

from flask import request, current_app, jsonify, safe_join, url_for
from werkzeug.utils import secure_filename

import upload_rest_api.database as db


def get_upload_path(project, fpath, root_upload_path=None):
    """Get upload path for current request"""
    if not root_upload_path:
        root_upload_path = current_app.config.get("UPLOAD_PATH")

    fpath, fname = os.path.split(fpath)
    fname = secure_filename(fname)
    project = secure_filename(project)

    joined_path = safe_join(root_upload_path, project)
    joined_path = safe_join(joined_path, fpath)

    return os.path.normpath(joined_path), fname


def get_project_path(project):
    """Get upload path for a given project"""
    root_upload_path = current_app.config.get("UPLOAD_PATH")
    project = secure_filename(project)

    return safe_join(root_upload_path, project)


def get_tmp_upload_path():
    """Get temporary unique upload path for tar and zip files"""

    tmp_upload_path = os.path.join(current_app.config.get("UPLOAD_TMP_PATH"))
    fpath = safe_join(tmp_upload_path, str(uuid.uuid4()))
    fpath, fname = os.path.split(fpath)

    return fpath, fname


def get_return_path(project, fpath, root_upload_path=None):
    """Splice upload_path and project from fpath and return the path
    shown to the user and POSTed to Metax.
    """
    if not root_upload_path:
        root_upload_path = current_app.config.get("UPLOAD_PATH")

    base_path = safe_join(root_upload_path, project)
    ret_path = os.path.normpath(fpath[len(base_path):])

    return ret_path if ret_path != "." else "/"


def make_response(status_code, message):
    """Returns jsonified default error message"""
    response = jsonify({"code": status_code, "error": message})
    response.status_code = status_code
    return response


def get_polling_url(name, task_id):
    """Creates url used to poll the status of asynchronous request"""
    path = url_for(name + ".task_status", task_id=task_id)
    parsed_url = urlparse(request.url)
    return urlunparse([parsed_url[0], parsed_url[1], path, "", "", ""])


def run_background(func):
    """ A decorator for running function on background"""
    @wraps(func)
    def _dec_func(*args, **kwargs):
        username = request.authorization.username
        database = db.Database()
        project = database.user(username).get_project()
        task_id = database.tasks.create(project)
        database.tasks.update_message(task_id, "processing")
        kwargs["task_id"] = task_id
        executor = current_app.config["EXTRACT_EXECUTOR"]
        executor.submit(func, *args, **kwargs)
        return task_id
    return _dec_func
