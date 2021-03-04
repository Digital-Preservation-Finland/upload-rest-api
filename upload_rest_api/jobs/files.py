"""Files API background jobs."""
from shutil import rmtree

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
import upload_rest_api.utils as utils
from upload_rest_api.config import CONFIG

from upload_rest_api.jobs.utils import api_background_job


@api_background_job
def delete_files(fpath, username, task_id):
    """Delete files and metadata denoted by fpath directory under user's
    project. The whole directory is recursively removed.

    :param str fpath: path to directory
    :param str username: current user
    :param str task_id: mongo dentifier of the task
    """
    root_upload_path = CONFIG["UPLOAD_PATH"]

    # Remove metadata from Metax
    metax_client = md.MetaxClient()
    database = db.Database()
    project = database.user(username).get_project()
    ret_path = utils.get_return_path(project, fpath, root_upload_path)
    database.tasks.update_message(
        task_id,
        "Deleting files and metadata: %s" % ret_path
    )
    metax_client.delete_all_metadata(project, fpath, root_upload_path)

    # Remove checksum from mongo
    database.checksums.delete_dir(fpath)

    # Remove project directory and update used_quota
    rmtree(fpath)
    database.user(username).update_used_quota(root_upload_path)

    database.tasks.update_message(
        task_id,
        "Deleted files and metadata: {}".format(ret_path)
    )
