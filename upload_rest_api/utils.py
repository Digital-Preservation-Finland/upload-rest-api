"""upload-rest-api utility functions."""
from pathlib import Path

from flask import request, url_for

try:
    from urllib.parse import urlparse, urlunparse
except ImportError:  # Python 2
    from urlparse import urlparse, urlunparse


def parse_user_path(root, *paths):
    """
    Check that the user-provided `path` is relative to `root` after resolving
    it to a full path and return the fully resolved path.

    :param root: Root path
    :param paths: User-provided relative path component(s).
                  This has to be relative to `root` after it has been resolved.
    :raises ValueError: If user-provided path is not relative to `root`
    :returns: Fully resolved path
    """
    # TODO: Replace with Path.is_relative_to in Python 3.9+
    root = Path(root).resolve()
    full_path = root.joinpath(*paths).resolve()

    # This will raise ValueError on paths that are not relative
    full_path.relative_to(root)

    return full_path


def parse_relative_user_path(path):
    """
    Parse a relative path returned by the user and return it in a sanitized
    form.

    :param path: Relative path returned by user
    :raises ValueError: If user-provided path attempts to escape root

    :returns: Safe relative path
    """
    # Will raise ValueError on attempted path escape
    path = parse_user_path(
        "/root_directory", path
    ).relative_to("/root_directory")
    path = str(path)

    # Ensure the path is returned without '.'
    if path == ".":
        path = ""

    return path


def get_polling_url(name, task_id):
    """Create url used to poll the status of asynchronous request."""
    path = url_for(name + ".task_status", task_id=task_id)
    parsed_url = urlparse(request.url)
    return urlunparse([parsed_url[0], parsed_url[1], path, "", "", ""])
