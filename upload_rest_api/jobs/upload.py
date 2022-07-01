"""Upload module background jobs."""
import os.path
import pathlib
import shutil
import uuid

from archive_helpers.extract import (ExtractError, MemberNameError,
                                     MemberOverwriteError, MemberTypeError,
                                     extract)
from metax_access import ResourceAlreadyExistsError

from upload_rest_api.config import CONFIG
import upload_rest_api.database as db
from upload_rest_api.checksum import get_file_checksum
from upload_rest_api.jobs.utils import (ClientError, api_background_job)
from upload_rest_api.lock import ProjectLockManager
from upload_rest_api import gen_metadata


def _process_extracted_files(fpath, storage_dir):
    """Process extracted files

    Unlinks all symlinks below fpath and calculates md5 checksums of all
    all files in fpath and returns a list of dicts::

        {
            "_id": filepath,
            "checksum": md5 digest
        }

    :param fpath: Path to the directory to be processed
    :param storage_dir: Path to storage directory
    :returns: A list of checksum dicts
    """
    checksums = []
    for dirpath, _, files in os.walk(fpath):
        for fname in files:
            _file = os.path.join(dirpath, fname)
            if os.path.islink(_file):
                os.unlink(_file)
                continue

            relative_path = pathlib.Path(_file).relative_to(fpath)
            target_path = storage_dir / relative_path
            if target_path.exists():
                # TODO: files should not be 'None'
                raise ClientError(f"File '{relative_path}' already exists",
                                  files=None)
            checksums.append({
                "_id": str(target_path),
                "checksum": get_file_checksum("md5", _file)
            })

    return checksums


def _move_extracted_files(fpath, storage_dir):
    """Move files to storage directory.

    :param fpath: Path to the directory to be processed
    :param storage_dir: Path to storage directory
    """
    for dirpath, _, files in os.walk(fpath):
        for fname in files:
            _file = os.path.join(dirpath, fname)
            relative_path = pathlib.Path(_file).relative_to(fpath)
            source_path = fpath / relative_path
            target_path = storage_dir / relative_path
            target_path.parent.mkdir(exist_ok=True, parents=True)
            source_path.rename(target_path)

            # TODO: Write permission for group is required by packaging
            # service (see https://jira.ci.csc.fi/browse/TPASPKT-516)
            os.chmod(target_path, 0o664)


@api_background_job
def store_file(project_id, tmp_path, path, task_id, file_type='file'):
    """Store file.

    This function creates the metadata in Metax for the file or
    directory denoted by path argument.

    :param str project_id: project identifier
    :param str tmp_path: path to source file/directory
    :param str path: target path of file/directory
    :param str task_id: identifier of the task
    """
    metax_client = gen_metadata.MetaxClient()
    database = db.Database()

    tmp_dir = pathlib.Path(CONFIG["UPLOAD_TMP_PATH"]) / str(uuid.uuid4())
    (tmp_dir / path).parent.mkdir(parents=True, exist_ok=True)
    project_path = db.Projects.get_project_directory(project_id)
    storage_path = project_path / path

    try:
        # Extract/move files to temporary path
        if file_type == 'archive':
            database.tasks.update_message(
                task_id, "Extracting archive"
            )
            try:
                extract(tmp_path, tmp_dir / path)
            except (MemberNameError, MemberTypeError, MemberOverwriteError,
                    ExtractError) as error:
                # Remove the archive and set task's state
                os.remove(tmp_path)
                raise ClientError(str(error)) from error

            # Remove archive
            os.remove(tmp_path)
        else:
            shutil.move(tmp_path, tmp_dir / path)

        # Add checksums of the extracted files to mongo
        database.files.insert(_process_extracted_files(tmp_dir, project_path))

        database.tasks.update_message(
            task_id, f"Creating metadata: {path}"
        )

        # Create metadata
        fpaths = []
        for dirpath, _, files in os.walk(tmp_dir / path):
            for fname in files:
                fpaths.append(os.path.join(dirpath, fname))

        try:
            metax_client.post_metadata(fpaths, tmp_dir, project_id)
        except ResourceAlreadyExistsError as error:
            try:
                failed_files = [file_['object']['file_path']
                                for file_ in error.response.json()['failed']]
            except KeyError:
                # Most likely only one file was posted so Metax response
                # format is different
                failed_files = [path]
            raise ClientError(error.message, files=failed_files) from error

        # Move files to project directory
        _move_extracted_files(tmp_dir, project_path)

        # Remove temporary directory. The directory might contain empty
        # directories, it must be removed recursively.
        shutil.rmtree(tmp_dir)

        # Update quota
        database.projects.update_used_quota(
            project_id, CONFIG["UPLOAD_PROJECTS_PATH"]
        )
    finally:
        lock_manager = ProjectLockManager()
        lock_manager.release(project_id, storage_path)

    return "Archive uploaded and extracted"
