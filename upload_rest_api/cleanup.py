"""Script for cleaning all the files from UPLOAD_DIR that haven't
been accessed within given time frame. This script can be set to run
periodically using cron.
"""
import os
import time
from runpy import run_path
import argparse

import upload_rest_api.gen_metadata as md
import upload_rest_api.database as db


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


def parse_conf(fpath):
    """Parse config from file fpath"""
    return run_path(fpath)


def _get_projects():
    """Returns a list of all projects with files uploaded"""
    conf = parse_conf("/etc/upload_rest_api.conf")
    upload_path = conf["UPLOAD_PATH"]
    dirs = []

    for _dir in os.listdir(upload_path):
        dirpath = os.path.join(upload_path, _dir)
        if os.path.isdir(dirpath):
            dirs.append(_dir)

    return dirs


def _clean_file(_file, upload_path, fpaths, file_dict=None, metax_client=None):
    """Remove file fpath from disk and add it to a list of files to be
    removed from if metax_client is provided and file is not associated
    with any datasets.

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


def clean_project(project, fpath, metax=True):
    """Remove all files of a given project that haven't been accessed
    within time_lim seconds. If the removed file has a Metax file entry and
    metax_client is provided, remove the Metax file entry as well.

    :param project: Project identifier used to search files from Metax
    :param fpath: Path to the dir to cleanup
    :param time_lim: Time limit in seconds
    :param metax: Boolean. if True metadata is removed also from Metax

    :returns: Number of deleted files
    """
    conf = parse_conf("/etc/upload_rest_api.conf")
    time_lim = conf["CLEANUP_TIMELIM"]
    upload_path = conf["UPLOAD_PATH"]

    current_time = time.time()
    metax_client = None
    file_dict = None
    fpaths = []
    deleted_count = 0

    if metax:
        metax_client = md.MetaxClient(
            url=conf["METAX_URL"],
            user=conf["METAX_USER"],
            password=conf["METAX_PASSWORD"]
        )
        file_dict = metax_client.get_files_dict(project)

    # Remove all old files
    for dirpath, _, files in os.walk(fpath):
        for fname in files:
            _file = os.path.join(dirpath, fname)
            if _is_expired(_file, current_time, time_lim):
                _clean_file(
                    _file, upload_path, fpaths,
                    file_dict, metax_client
                )
                deleted_count += 1

    # Remove all empty dirs
    _clean_empty_dirs(fpath)

    # Remove Metax file entries of deleted files
    if metax:
        metax_client.delete_metadata(project, fpaths)

    return deleted_count


def clean_disk(metax=True):
    """Clean all project in upload_path

    :returns: Count of deleted files
    """
    conf = parse_conf("/etc/upload_rest_api.conf")
    upload_path = conf["UPLOAD_PATH"]
    deleted_count = 0

    projects = os.listdir(upload_path)
    for project in projects:
        fpath = os.path.join(upload_path, project)
        deleted_count += clean_project(project, fpath, metax)

    return deleted_count


def clean_ida():
    """Remove all files from /var/spool/siptools_research/ida_files
    that haven't been accessed in two weeks.

    :returns: Count of deleted files
    """
    ida_files_path = "/var/spool/siptools_research/ida_files"
    current_time = time.time()
    time_lim = 60*60*24*14
    deleted_count = 0

    # Remove all old files
    for dirpath, _, files in os.walk(ida_files_path):
        for fname in files:
            _file = os.path.join(dirpath, fname)
            if _is_expired(_file, current_time, time_lim):
                os.remove(_file)
                deleted_count += 1

    return deleted_count


def clean_mongo():
    """Clean file identifiers that do not exist in Metax any more from Mongo

    :returns: Count of cleaned Mongo documents
    """
    files = db.FilesCol()
    projects = _get_projects()

    conf = parse_conf("/etc/upload_rest_api.conf")
    url = conf["METAX_URL"]
    user = conf["METAX_USER"]
    password = conf["METAX_PASSWORD"]

    metax_ids = md.MetaxClient(url, user, password).get_all_ids(projects)

    files = db.FilesCol()
    mongo_ids = files.get_all_ids()
    id_list = []

    # Check for identifiers found in Mongo but not in Metax
    for identifier in mongo_ids:
        if identifier not in metax_ids:
            id_list.append(identifier)

    # Remove identifiers from mongo
    return files.delete(id_list)


def _parse_arguments(arguments):
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Clean files from disk or identfiers from mongo."
    )
    parser.add_argument("location", type=str, help="mongo, disk or ida_files")

    return parser.parse_args(arguments)


def main(arguments=None):
    """Parse command line arguments and clean disk or mongo"""
    args = _parse_arguments(arguments)

    if args.location == "disk":
        deleted_count = clean_disk()
    elif args.location == "mongo":
        deleted_count = clean_mongo()
    elif args.location == "ida_files":
        deleted_count = clean_ida()
    else:
        raise Exception("Unsupported location: %s" % args.location)

    print "Cleaned %d files" % deleted_count


if __name__ == "__main__":
    main()
