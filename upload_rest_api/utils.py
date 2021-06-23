"""upload-rest-api utility functions."""
import os
import uuid
try:
    from urllib.parse import urlparse, urlunparse
except ImportError:  # Python 2
    from urlparse import urlparse, urlunparse

from flask import request, current_app, jsonify, safe_join, url_for
from werkzeug.utils import secure_filename


from upload_rest_api.config import CONFIG


def get_upload_path(project, file_path, root_upload_path=None):
    """Get upload path for file.

    :param project: project identifier
    :param file_path: relative file path
    :param root_upload_path: root upload path
    :returns: tuple that contains real path of directory and file
              name
    """
    if not root_upload_path:
        root_upload_path = CONFIG.get("UPLOAD_PATH")

    fpath, fname = os.path.split(file_path)
    secure_fname = secure_filename(fname)
    secure_project = secure_filename(project)

    joined_path = safe_join(root_upload_path, secure_project)
    joined_path = safe_join(joined_path, fpath)

    return os.path.normpath(joined_path), secure_fname


def get_tmp_upload_path():
    """Get temporary unique upload path for tar and zip files."""
    tmp_upload_path = os.path.join(CONFIG.get("UPLOAD_TMP_PATH"))
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
    """Return jsonified default error message."""
    response = jsonify({"code": status_code, "error": message})
    response.status_code = status_code
    return response


def get_polling_url(name, task_id):
    """Create url used to poll the status of asynchronous request."""
    path = url_for(name + ".task_status", task_id=task_id)
    parsed_url = urlparse(request.url)
    return urlunparse([parsed_url[0], parsed_url[1], path, "", "", ""])
