"""Script for cleaning all the files from UPLOAD_DIR that haven't
been accessed within given time frame. This script can be set to run
periodically using cron.
"""
import os
import time

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


def _parse_conf(fpath, params):
    """Parse config from file fpath for parameters params"""
    conf = {}

    with open(fpath) as _file:
        for line in _file:
            # Remove everything after first #
            line = line.split("#")[0]
            split = line.split("=")

            if len(split) == 2:
                key = split[0].strip()
                value = split[1].strip()

                if key in params:
                    conf[key] = value[1:-1]

    return conf


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
    current_time = time.time()
    fpaths = []

    if metax:
        conf = _parse_conf(
            "/etc/upload_rest_api.conf",
            {"UPLOAD_PATH", "METAX_URL", "METAX_USER", "METAX_PASSWORD"}
        )

    # Remove all old files
    for dirpath, _, files in os.walk(fpath):
        for fname in files:
            _file = os.path.join(dirpath, fname)
            if _is_expired(_file, current_time, time_lim):
                os.remove(_file)

                if metax:
                    fpaths.append(md.get_metax_path(
                        _file, conf["UPLOAD_PATH"]
                    ))

    # Remove all empty dirs
    _clean_empty_dirs(fpath)

    # Remove Metax file entries of deleted files
    if metax:
        url = conf["METAX_URL"]
        user = conf["METAX_USER"]
        password = conf["METAX_PASSWORD"]

        md.delete_metadata(
            project, fpaths,
            md.get_metax_client(url=url, user=user, password=password)
        )
