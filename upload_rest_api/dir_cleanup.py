"""Script for cleaning all the files from UPLOAD_DIR that haven't
been accessed within given time frame. This script can be set to run
periodically using cron.
"""
import os
import time
from runpy import run_path

import upload_rest_api.gen_metadata as md


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


def _parse_conf(fpath):
    """Parse config from file fpath"""
    return run_path(fpath)


def _clean_file(_file, upload_path, fpaths, file_dict=None, metax_client=None):
    """Remove file fpath from disk and add it to a list of files to be
    removed from if metax_client is provided

    :param fpath: Path to where the file is stored in disk
    :param upload_path: Base path used for uploading the files
    :param fpaths: List files to be removed from Metax are appended

    :returns: None
    """
    if metax_client is not None and file_dict is not None:
        metax_path = md.get_metax_path(_file, upload_path)

        if not metax_client.file_has_dataset(metax_path, file_dict):
            fpaths.append(metax_path)
            os.remove(_file)
    else:
        os.remove(_file)


def cleanup(project, fpath, time_lim, metax=True):
    """Remove all files that haven't been accessed within time_lim seconds.
    If the removed file has a Metax file entry and metax_client is provided,
    remove the Metax file entry as well.

    :param project: Project identifier used to search files from Metax
    :param fpath: Path to the dir to cleanup
    :param time_lim: Time limit in seconds
    :param metax: Boolean. if True metadata is removed also from Metax

    :return: None
    """
    conf = _parse_conf("/etc/upload_rest_api.conf")
    current_time = time.time()
    metax_client = None
    file_dict = None
    fpaths = []

    if metax:
        url = conf["METAX_URL"]
        user = conf["METAX_USER"]
        password = conf["METAX_PASSWORD"]

        metax_client = md.MetaxClient(url=url, user=user, password=password)
        file_dict = metax_client.get_files_dict(project)

    # Remove all old files
    for dirpath, _, files in os.walk(fpath):
        for fname in files:
            _file = os.path.join(dirpath, fname)
            if _is_expired(_file, current_time, time_lim):
                _clean_file(
                    _file, conf["UPLOAD_PATH"], fpaths,
                    file_dict, metax_client
                )

    # Remove all empty dirs
    _clean_empty_dirs(fpath)

    # Remove Metax file entries of deleted files
    if metax:
        metax_client.delete_metadata(project, fpaths)

cleanup("test_project", "/home/vagrant/test/rest/test_project", 1)
