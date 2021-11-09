"""upload-rest-api utility functions."""
import os
import pathlib

from flask import request, safe_join, url_for
from upload_rest_api.database import Projects
from werkzeug.utils import secure_filename

try:
    from urllib.parse import urlparse, urlunparse
except ImportError:  # Python 2
    from urlparse import urlparse, urlunparse


def get_upload_path(project_id, file_path):
    """Get upload path for file.

    :param project_id: project identifier
    :param file_path: file path relative to project directory of user
    :returns: full path of file
    """
    dirname, basename = os.path.split(file_path)
    secure_fname = secure_filename(basename)
    joined_path = safe_join(Projects.get_project_directory(project_id), dirname)

    return pathlib.Path(joined_path).resolve() / secure_fname


def get_return_path(project_id, fpath):
    """Get path relative to project directory.

    Splice project path from fpath and return the path shown to the user
    and POSTed to Metax.

    :param project_id: project identifier
    :param fpath: full path
    :returns: string presentation of relative path
    """
    path = pathlib.Path(fpath).relative_to(
        Projects.get_project_directory(project_id)
    )

    path_string = f"/{path}" if path != pathlib.Path('.') else '/'

    return path_string


def get_polling_url(name, task_id):
    """Create url used to poll the status of asynchronous request."""
    path = url_for(name + ".task_status", task_id=task_id)
    parsed_url = urlparse(request.url)
    return urlunparse([parsed_url[0], parsed_url[1], path, "", "", ""])
