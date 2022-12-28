"""Functions for cleaning old data."""
# TODO: Probably almost all functionality in this module should be
# implemented in models
import datetime
import errno
import logging
import os
import pathlib
import time

import upload_rest_api.config
import upload_rest_api.gen_metadata as md
from upload_rest_api.lock import ProjectLockManager
from upload_rest_api.models.file_entry import FileEntry
from upload_rest_api.models.task import Task
from upload_rest_api.models.upload import Upload, UploadEntry

# This is the time-to-live for upload database entries *in addition* to the
# upload lock TTL. This ensures that longer uploads are given time to complete
# even if they might exceed the lock lifetime.
NON_TUS_UPLOAD_TTL = datetime.timedelta(days=2)


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
def _clean_project(project_id, fpath, metax=True):
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
    FileEntry.objects.filter(path__in=deleted_files).delete()

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
        deleted_count += _clean_project(project, fpath, metax)

    return deleted_count


def clean_mongo():
    """Clean old tasks from Mongo.

    :returns: Count of cleaned Mongo documents
    """
    conf = upload_rest_api.config.CONFIG
    time_lim = conf["CLEANUP_TIMELIM"]
    Task.clean_old_tasks(time_lim)


def clean_tus_uploads():
    """
    Clean aborted tus uploads

    Aborted tus uploads are cleared once they no longer have a corresponding
    tus workspace on disk. This is because they have been cleaned after
    remaining inactive for 4 hours by a background service.
    """
    conf = upload_rest_api.config.CONFIG
    tus_spool_dir = pathlib.Path(conf["TUS_API_SPOOL_PATH"])

    resource_ids_on_disk = {path.name for path in tus_spool_dir.iterdir()}
    resource_ids_on_mongo = {
        str(entry["_id"]) for entry
        in UploadEntry.objects.filter(is_tus_upload=True)
                      .only("id").as_pymongo()
    }

    resource_ids_to_delete = list(resource_ids_on_mongo - resource_ids_on_disk)

    lock_manager = ProjectLockManager()

    uploads_to_delete = UploadEntry.objects.filter(
        id__in=resource_ids_to_delete
    )
    # Create Upload instances manually. Retrieving them one-by-one using
    # `Upload.get` results in multiple unnecessary queries.
    uploads_to_delete = [
        Upload(db_upload=db_upload) for db_upload in uploads_to_delete
    ]
    for upload in uploads_to_delete:
        try:
            lock_manager.release(upload.project.id, upload.storage_path)
        except ValueError:
            # Cleanup should happen before the lock expires.
            # If the lock still exists, the cleanup was probably delayed for
            # some reason.
            logging.warning(
                "Lock for %s/%s has already expired, ignoring. "
                "Was the cleanup delayed for some reason?",
                upload.project.id, upload.storage_path
            )

    deleted_count = \
        UploadEntry.objects.filter(id__in=resource_ids_to_delete).delete()

    return deleted_count


def clean_other_uploads():
    """Clean likely aborted uploads from the MongoDB database.

    Uploads older than 2 days after expired locks are deleted from the
    database, as it's likely the upload has crashed at that point.
    """
    lock_manager = ProjectLockManager()

    # The cutoff is the default lock TTL with additional two days to ensure
    # uploads exceeding the TTL have plenty of time to succeed.
    cutoff = (
        datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(seconds=lock_manager.default_lock_ttl)
        - NON_TUS_UPLOAD_TTL
    )

    # We don't need to deal with locks here, as they have expired at this
    # point.
    deleted_count = UploadEntry.objects.filter(
        is_tus_upload=False, started_at__lte=cutoff
    ).delete()

    return deleted_count
