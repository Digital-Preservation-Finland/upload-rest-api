"""Upload module background jobs."""
from upload_rest_api.database import Task
from upload_rest_api.jobs.utils import ClientError, api_background_job
from upload_rest_api.upload import (InvalidArchiveError, Upload, UploadError,
                                    continue_upload)


@api_background_job
def store_files(project_id, path, upload_type, identifier, task_id):
    """Store files.

    Create metadata for uploaded files and move them to storage.

    :param str project_id: project identifier
    :param str path: upload path
    :param str upload_type: Type of upload ("file" or "archive")
    :param str upload_id: identifier of upload
    :param str task_id: identifier of the task
    """
    upload = continue_upload(project_id, path, upload_type=upload_type,
                             identifier=identifier)

    if upload_type == 'archive':
        try:
            # TODO: Archive validation was moved here because, because
            # in some cases checking conflicts seems to be too slow to
            # be done synchronously. Validating archive type and size
            # are probably fast enough to be done synchronously.
            upload.validate_archive()
        except UploadError as error:
            raise ClientError(str(error)) from error
        message = "Extracting archive"
    else:
        message = f"Creating metadata /{upload.path}"

    Task.objects.filter(id=task_id).update_one(message=message)

    try:
        upload.store_files()
    except UploadError as error:
        raise ClientError(str(error)) from error

    return f"{upload_type} uploaded to {upload.path}"
