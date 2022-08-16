"""Upload module background jobs."""
import upload_rest_api.database
from upload_rest_api.upload import Upload, InvalidArchiveError
from upload_rest_api.jobs.utils import api_background_job, ClientError


@api_background_job
def store_files(project_id, path, upload_type, upload_id, task_id):
    """Store files.

    Create metadata for uploaded files and move them to storage.

    :param str project_id: project identifier
    :param str path: upload path
    :param str upload_type: Type of upload ("file" or "archive")
    :param str upload_id: identifier of upload
    :param str task_id: identifier of the task
    """
    database = upload_rest_api.database.Database()
    upload = Upload(project_id, path, upload_type=upload_type,
                    upload_id=upload_id)

    if upload_type == 'archive':
        database.tasks.update_message(task_id, "Extracting archive")
    else:
        database.tasks.update_message(
            task_id, f"Creating metadata /{upload.path}"
        )

    try:
        upload.store_files()
    except InvalidArchiveError as error:
        raise ClientError(error.message) from error

    return f"{upload_type} uploaded to /{upload.path}"
