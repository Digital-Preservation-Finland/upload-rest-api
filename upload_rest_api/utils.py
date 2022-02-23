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
    :param path: User-provided relative path. This has to be relative to
                 `root` after it has been resolved.
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


def get_upload_path(project_id, file_path):
    """Get upload path for file/directory.

    :param project_id: project identifier
    :param file_path: file path relative to project directory of user
    :returns: full path of file/directory
    """
    # Prevent circular import
    from upload_rest_api.database import Projects

    if file_path == "*":
        # '*' is shorthand for the base directory.
        # This is used to maintain compatibility with Werkzeug's
        # 'secure_filename' function that would sanitize it into an empty
        # string.
        file_path = ""

    project_dir = Projects.get_project_directory(project_id)
    upload_path = (project_dir / file_path).resolve()

    return parse_user_path(project_dir, upload_path)


def get_trash_path(project_id, file_path, trash_id):
    """Get trash path for file/directory.

    This is the temporary path where the directory will be moved atomically
    in order to perform the actual deletion.

    :param project_id: project identifier
    :param file_path: file path relative to project directory of user
    :returns: full path of file/directory
    """
    from upload_rest_api.database import Projects

    return parse_user_path(
        Projects.get_trash_directory(
            project_id=project_id, trash_id=trash_id
        ),
        file_path
    )


def get_return_path(project_id, fpath):
    """Get path relative to project directory.

    Splice project path from fpath and return the path shown to the user
    and POSTed to Metax.

    :param project_id: project identifier
    :param fpath: full path
    :returns: string presentation of relative path
    """
    # Prevent circular import
    from upload_rest_api.database import Projects

    if fpath == "*":
        # '*' is shorthand for the base directory.
        # This is used to maintain compatibility with Werkzeug's
        # 'secure_filename' function that would sanitize it into an empty
        # string
        fpath = ""

    path = Path(fpath).relative_to(
        Projects.get_project_directory(project_id)
    )

    path_string = f"/{path}" if path != Path('.') else '/'

    return path_string


def get_polling_url(name, task_id):
    """Create url used to poll the status of asynchronous request."""
    path = url_for(name + ".task_status", task_id=task_id)
    parsed_url = urlparse(request.url)
    return urlunparse([parsed_url[0], parsed_url[1], path, "", "", ""])
