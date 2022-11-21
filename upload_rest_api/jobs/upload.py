"""Upload module background jobs."""
from upload_rest_api.jobs.utils import ClientError, api_background_job
from upload_rest_api.models import Upload, UploadError, UploadType


@api_background_job
def store_files(identifier, task):
    """Store files.

    Create metadata for uploaded files and move them to storage.

    :param str identifier: identifier of upload
    :param str task: Task instance
    """
    upload = Upload.get(id=identifier)

    if upload.type_ == UploadType.ARCHIVE:
        message = "Extracting archive"
    else:
        message = f"Creating metadata /{upload.path}"

    task.set_fields(message=message)

    try:
        # TODO: Archive validation was moved here because, because
        # in some cases checking conflicts seems to be too slow to
        # be done synchronously. Validating archive type and size
        # are probably fast enough to be done synchronously.
        upload.validate_archive()

        upload.store_files()
    except UploadError as error:
        raise ClientError(str(error)) from error

    return f"{upload.type_.value} uploaded to {upload.path}"
