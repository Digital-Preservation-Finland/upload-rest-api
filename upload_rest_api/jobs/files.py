"""Files API background jobs."""
import shutil
import os

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
import upload_rest_api.utils as utils
from upload_rest_api.config import CONFIG

from upload_rest_api.jobs.utils import api_background_job


def _get_files_to_delete(trash_path, trash_root, project_dir):
    """
    Get files to delete from the checksums database.

    Each path is converted to the original absolute path in the format
    "/var/spool/upload/projects/<project_id>". This is necessary because
    the database uses absolute paths as identifiers.
    """
    filepaths = []
    for _dir, _, files in os.walk(trash_path):
        for _file in files:
            fpath = os.path.join(_dir, _file)
            fpath = os.path.abspath(fpath)

            # Change the path
            # from `<spool_path>/trash/<trash_id>/<project_id>`
            # to `<spool_path>/upload/<project_id>`
            fpath = f"{project_dir}{fpath[len(str(trash_root)):]}"
            filepaths.append(fpath)

    return filepaths


@api_background_job
def delete_files(fpath, trash_path, trash_root, project_id, task_id):
    """Delete files and metadata denoted by fpath directory under temporary
    directory. The whole directory is recursively removed after Metax metadata
    is removed.

    :param pathlib.Path fpath: path to the original directory
    :param pathlib.Path trash_path: path to the temporary trash directory
    :param pathlib.Path trash_root: root of the temporary trash directory,
                                    corresponding to
                                    `<spool_path>/<trash_id>/<project_id>`
    :param str project: project identifier
    :param str task_id: mongo dentifier of the task
    """
    root_upload_path = CONFIG["UPLOAD_PATH"]

    # Remove metadata from Metax
    metax_client = md.MetaxClient()
    database = db.Database()
    project_dir = database.projects.get_project_directory(project_id)
    ret_path = utils.get_return_path(project_id, fpath)
    database.tasks.update_message(
        task_id,
        "Deleting files and metadata: %s" % ret_path
    )
    metax_client.delete_all_metadata(project_id,
                                     trash_path,
                                     trash_root)

    # Remove checksum from mongo
    files_to_delete = _get_files_to_delete(
        trash_path=trash_path,
        trash_root=trash_root,
        project_dir=project_dir
    )
    database.checksums.delete(files_to_delete)

    # Ensure the directory containing the "fake" project directory is
    # deleted as well.
    shutil.rmtree(trash_root.parent)

    # Update used_quota
    database.projects.update_used_quota(project_id, root_upload_path)

    return f"Deleted files and metadata: {ret_path}"
