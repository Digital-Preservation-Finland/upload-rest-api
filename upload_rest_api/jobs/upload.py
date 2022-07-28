"""Upload module background jobs."""
import shutil

import upload_rest_api.database
import upload_rest_api.upload
from upload_rest_api.jobs.utils import api_background_job


@api_background_job
def store_files(project_id, tmp_path, path, file_type, task_id):
    """Store files.

    Create metadata for uploaded files and move them to storage.

    :param str project_id: project identifier
    :param str tmp_path: path to source file/directory
    :param str path: target path of file/directory
    :param str task_id: identifier of the task
    """
    database = upload_rest_api.database.Database()
    database.tasks.update_message(
        task_id, f"Creating metadata: {path}"
    )

    upload = upload_rest_api.upload.Upload(project_id, path, tmp_path)
    if file_type == 'archive':
        database.tasks.update_message(task_id, "Extracting archive")
        upload.extract_archive()
        database.tasks.update_message(task_id, "Archive extracted")
    else:
        # The source file is not an archive. Just move the file to
        # temporary project directory.
        (upload.tmp_project_directory / upload.path).parent.mkdir(
            parents=True, exist_ok=True
        )
        shutil.move(upload.source_path,
                    upload.tmp_project_directory / upload.path)

    database.tasks.update_message(
        task_id, f"Creating metadata: {upload.path}"
    )

    upload.store_files()

    return f"{file_type} uploaded to /{upload.path}"
