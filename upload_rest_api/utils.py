"""upload-rest-api utility functions."""
from pathlib import Path


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
