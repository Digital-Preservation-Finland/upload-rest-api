"""Upload module background jobs."""
from upload_rest_api.jobs.utils import ClientError, api_background_job
from upload_rest_api.models import Upload, UploadError, UploadType


@api_background_job
def store_files(identifier, verify_source, task):
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
        upload.store_files(verify_source)
    except UploadError as error:
        raise ClientError(str(error), error.files) from error

    return f"{upload.type_.value} uploaded to {upload.path}"
