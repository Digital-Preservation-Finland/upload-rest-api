"""Script for cleaning all the files from UPLOAD_DIR that haven't
been accessed within given time frame. This script can be set to run
periodically using cron.
"""
import os
import time


def _is_expired(fpath, current_time, time_lim):
    """Checks last file access and calculates whether the
    file is considered expired or not.

    :param fpath: Path to the file
    :param current_time: Current Unix time
    :param time_lim: Time limit in seconds

    :returns: True is expired else False
    """
    last_access = os.stat(fpath).st_atime

    return current_time - last_access > time_lim


def _clean_empty_dirs(fpath):
    """Remove all directories, which have no files anymore."""
    for dirpath, _, _ in os.walk(fpath, topdown=False):
        # Do not remove UPLOAD_FOLDER itself
        if dirpath == fpath:
            break

        # Try removing the directory
        try:
            os.rmdir(dirpath)
        except OSError as err:
            # Raise all errors except [Errno 39] Directory not empty
            if err.errno != 39:
                raise


def cleanup(fpath, time_lim):
    """Remove all files that haven't been accessed within time_lim seconds

    :param fpath: Path to the dir to cleanup
    :param time_lim: Time limit in seconds

    :return: None
    """
    current_time = time.time()

    # Remove all old files
    for dirpath, _, files in os.walk(fpath):
        for fname in files:
            _file = os.path.join(dirpath, fname)
            if _is_expired(_file, current_time, time_lim):
                os.remove(_file)

    # Remove all empty dirs
    _clean_empty_dirs(fpath)
