"""Script for cleaning all the files from UPLOAD_DIR that haven't
been accessed within given time frame. This script can be set to run
periodically using cron.
"""
import errno
import os
import time

import upload_rest_api.config
import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md


def _is_expired(fpath, current_time, time_lim):
    """Check last file access and calculate whether the file is
    considered expired or not.

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
            if err.errno != errno.ENOTEMPTY:
                raise


def _clean_old_tasks(time_lim):
    """Remove tasks that are older than time_lim.

    :param time_lim: : expiration time in seconds
    """
    current_time = time.time()
    tasks = db.Database().tasks
    for task in tasks.get_all_tasks():
        if current_time - task["timestamp"] > time_lim:
            tasks.delete_one(task["_id"])


def _get_projects():
    """Return a list of all projects with files uploaded."""
    conf = upload_rest_api.config.CONFIG
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
    within time_lim seconds. If the removed file has a Metax file entry
    and metax_client is provided, remove the Metax file entry as well.

    :param project: Project identifier used to search files from Metax
    :param fpath: Path to the dir to cleanup
    :param time_lim: Time limit in seconds
    :param metax: Boolean. if True metadata is removed also from Metax

    :returns: Number of deleted files
    """
    conf = upload_rest_api.config.CONFIG
    time_lim = conf["CLEANUP_TIMELIM"]
    upload_path = conf["UPLOAD_PATH"]

    current_time = time.time()
    metax_client = None
    file_dict = None
    fpaths = []
    deleted_files = []

    if metax:
        metax_client = md.MetaxClient(
            url=conf["METAX_URL"],
            user=conf["METAX_USER"],
            password=conf["METAX_PASSWORD"],
            verify=conf["METAX_SSL_VERIFICATION"]
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
                deleted_files.append(_file)

    # Remove all empty dirs
    _clean_empty_dirs(fpath)

    # Clean checksums of the deleted files from mongo
    db.Database().checksums.delete(deleted_files)

    # Remove Metax entries of deleted files that are not part of any
    # datasets
    if metax:
        metax_client.delete_metadata(project, fpaths)

    return len(deleted_files)


def clean_disk(metax=True):
    """Clean all project in upload_path.

    :returns: Count of deleted files
    """
    conf = upload_rest_api.config.CONFIG
    upload_path = conf["UPLOAD_PATH"]
    deleted_count = 0

    projects = os.listdir(upload_path)
    for project in projects:
        fpath = os.path.join(upload_path, project)
        deleted_count += clean_project(project, fpath, metax)

    return deleted_count


def clean_mongo():
    """Clean old tasks from mongo.

    Clean file identifiers that do not exist in Metax any more from
    Mongo.

    :returns: Count of cleaned Mongo documents
    """
    conf = upload_rest_api.config.CONFIG
    url = conf["METAX_URL"]
    user = conf["METAX_USER"]
    password = conf["METAX_PASSWORD"]
    ssl_verification = conf["METAX_SSL_VERIFICATION"]
    time_lim = conf["CLEANUP_TIMELIM"]

    _clean_old_tasks(time_lim)

    projects = _get_projects()

    metax_ids = md.MetaxClient(url,
                               user,
                               password,
                               ssl_verification).get_all_ids(projects)

    files = db.Database().files
    mongo_ids = files.get_all_ids()
    id_list = []

    # Check for identifiers found in Mongo but not in Metax
    for identifier in mongo_ids:
        if identifier not in metax_ids:
            id_list.append(identifier)

    # Remove identifiers from mongo
    return files.delete(id_list)
