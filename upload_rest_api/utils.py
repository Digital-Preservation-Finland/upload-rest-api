"""upload-rest-api utility functions."""
import os
import pathlib

try:
    from urllib.parse import urlparse, urlunparse
except ImportError:  # Python 2
    from urlparse import urlparse, urlunparse

from flask import request, safe_join, url_for
from werkzeug.utils import secure_filename


from upload_rest_api.config import CONFIG


def get_upload_path(user, file_path):
    """Get upload path for file.

    :param user: user object
    :param file_path: file path relative to project directory of user
    :returns: full path of file
    """
    dirname, basename = os.path.split(file_path)
    secure_fname = secure_filename(basename)
    joined_path = safe_join(user.project_directory, dirname)

    return pathlib.Path(joined_path).resolve() / secure_fname


def get_return_path(user, fpath):
    """Get path relative to project directory of user.

    Splice project path from fpath and return the path shown to the user
    and POSTed to Metax.

    :param user: user object
    :param fpath: full path
    :returns: string presentation of relative path
    """
    path = pathlib.Path(fpath).relative_to(user.project_directory)

    path_string = f"/{path}" if path != pathlib.Path('.') else '/'

    return path_string


def get_polling_url(name, task_id):
    """Create url used to poll the status of asynchronous request."""
    path = url_for(name + ".task_status", task_id=task_id)
    parsed_url = urlparse(request.url)
    return urlunparse([parsed_url[0], parsed_url[1], path, "", "", ""])
