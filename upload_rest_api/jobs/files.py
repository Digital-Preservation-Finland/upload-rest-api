"""Files API background jobs."""
import shutil

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
import upload_rest_api.utils as utils
from upload_rest_api.config import CONFIG

from upload_rest_api.jobs.utils import api_background_job


@api_background_job
def delete_files(fpath, project_id, task_id):
    """Delete files and metadata denoted by fpath directory under user's
    project. The whole directory is recursively removed.

    :param pathlib.Path fpath: path to directory
    :param str project: project identifier
    :param str task_id: mongo dentifier of the task
    """
    root_upload_path = CONFIG["UPLOAD_PATH"]

    # Remove metadata from Metax
    metax_client = md.MetaxClient()
    database = db.Database()
    ret_path = utils.get_return_path(project_id, fpath)
    database.tasks.update_message(
        task_id,
        "Deleting files and metadata: %s" % ret_path
    )
    metax_client.delete_all_metadata(project_id,
                                     fpath,
                                     root_upload_path)

    # Remove checksum from mongo
    database.checksums.delete_dir(fpath)

    project_dir = db.Projects.get_project_directory(project_id)

    # Remove files
    if fpath.samefile(project_dir):
        # Remove all content of project directory
        for child in fpath.iterdir():
            if child.is_file():
                child.unlink()
            else:
                shutil.rmtree(child)
    else:
        # Remove the whole directory
        shutil.rmtree(fpath)

    # Update used_quota
    database.projects.update_used_quota(project_id, root_upload_path)

    return f"Deleted files and metadata: {ret_path}"
