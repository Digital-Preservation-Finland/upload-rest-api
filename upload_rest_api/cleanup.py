"""Script for cleaning all the files from UPLOAD_DIR that haven't
been accessed within given time frame. This script can be set to run
periodically using cron.
"""
import errno
import os
import pathlib
import time

import upload_rest_api.config
import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
from upload_rest_api.lock import ProjectLockManager


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


def _get_projects():
    """Return a list of all projects with files uploaded."""
    conf = upload_rest_api.config.CONFIG
    upload_path = conf["UPLOAD_PROJECTS_PATH"]
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
        metax_path = md.get_metax_path(pathlib.Path(_file), upload_path)

        if not metax_client.file_has_dataset(metax_path, file_dict):
            fpaths.append(metax_path)
            os.remove(_file)
    else:
        os.remove(_file)


# pylint: disable=too-many-locals
def clean_project(project_id, fpath, metax=True):
    """Remove all files of a given project that haven't been accessed
    within time_lim seconds. If the removed file has a Metax file entry
    and metax_client is provided, remove the Metax file entry as well.

    :param project_id: Project identifier used to search files from Metax
    :param fpath: Path to the dir to cleanup
    :param metax: Boolean. if True metadata is removed also from Metax

    :returns: Number of deleted files
    """
    conf = upload_rest_api.config.CONFIG
    time_lim = conf["CLEANUP_TIMELIM"]
    upload_path = conf["UPLOAD_PROJECTS_PATH"]

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
        file_dict = metax_client.get_files_dict(project_id)

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

    # Clean deleted files from mongo
    db.Database().files.delete(deleted_files)

    # Remove Metax entries of deleted files that are not part of any
    # datasets
    if metax:
        metax_client.delete_metadata(project_id, fpaths)

    return len(deleted_files)


def clean_disk(metax=True):
    """Clean all project in upload_path.

    :returns: Count of deleted files
    """
    conf = upload_rest_api.config.CONFIG
    upload_path = conf["UPLOAD_PROJECTS_PATH"]
    deleted_count = 0

    projects = os.listdir(upload_path)
    for project in projects:
        fpath = os.path.join(upload_path, project)
        deleted_count += clean_project(project, fpath, metax)

    return deleted_count


def clean_mongo():
    """Clean old tasks from Mongo.

    :returns: Count of cleaned Mongo documents
    """
    conf = upload_rest_api.config.CONFIG
    time_lim = conf["CLEANUP_TIMELIM"]

    current_time = time.time()
    tasks = db.Database().tasks
    for task in tasks.get_all_tasks():
        if current_time - task["timestamp"] > time_lim:
            tasks.delete_one(task["_id"])


def clean_tus_uploads():
    """Clean aborted tus uploads from the MongoDB database.

    Aborted tus uploads are uploads that no longer have a corresponding
    tus workspace on disk. This is because they have been cleaned after
    remaining inactive for 4 hours. The corresponding upload entry on MongoDB
    has to be deleted as well; otherwise the user will be unable to upload
    a file into the same directory.
    """
    conf = upload_rest_api.config.CONFIG
    tus_spool_dir = pathlib.Path(conf["TUS_API_SPOOL_PATH"])

    database = db.Database()

    resource_ids_on_disk = {path.name for path in tus_spool_dir.iterdir()}
    resource_ids_on_mongo = {
        str(entry["_id"]) for entry in database.uploads.uploads.find()
    }

    resource_ids_to_delete = list(resource_ids_on_mongo - resource_ids_on_disk)

    lock_manager = ProjectLockManager()
    for resource_id in resource_ids_to_delete:
        upload = database.uploads.uploads.find_one({"_id": resource_id})
        if upload:
            lock_manager.release(upload['project'], upload['upload_path'])

    deleted_count = database.uploads.delete(resource_ids_to_delete)

    return deleted_count
