"""Functions for cleaning old data."""
# TODO: Probably almost all functionality in this module should be
# implemented in models
import datetime
import logging
import pathlib

import upload_rest_api.config
from upload_rest_api.lock import ProjectLockManager
from upload_rest_api.models.project import Project
from upload_rest_api.models.task import Task
from upload_rest_api.models.upload import Upload, UploadEntry
from upload_rest_api.models.resource import get_resource

# This is the time-to-live for upload database entries *in addition* to the
# upload lock TTL. This ensures that longer uploads are given time to complete
# even if they might exceed the lock lifetime.
NON_TUS_UPLOAD_TTL = datetime.timedelta(days=2)


def clean_disk():
    """Delete all expired files.

    :returns: Count of deleted files
    """
    deleted_count = 0

    for project in Project.list_all():
        project_directory = get_resource(project.id, '/')
        deleted_count += project_directory.delete_expired_files()

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
