"""Upload module background jobs."""
import os.path
import pathlib
import shutil
import uuid
import tarfile
import zipfile

from archive_helpers.extract import (ExtractError, MemberNameError,
                                     MemberOverwriteError, MemberTypeError,
                                     extract)
from flask import safe_join
from metax_access import ResourceAlreadyExistsError

from upload_rest_api.config import CONFIG
import upload_rest_api.database as db
from upload_rest_api.checksum import get_file_checksum
from upload_rest_api.jobs.utils import (ClientError, api_background_job)
from upload_rest_api.lock import ProjectLockManager
from upload_rest_api import gen_metadata


def _process_extracted_files(fpath):
    """Unlink all symlinks below fpath and change the mode of all other
    regular files to 0o664.

    :param fpath: Path to the directory to be processed
    :returns: None
    """
    for dirpath, _, files in os.walk(fpath):
        for fname in files:
            _file = os.path.join(dirpath, fname)
            if os.path.islink(_file):
                os.unlink(_file)
            elif os.path.isfile(_file):
                os.chmod(_file, 0o664)


def _get_archive_checksums(archive, extract_path):
    """Calculate md5 checksums of all archive members and return a list
    of dicts::

        {
            "_id": filpath,
            "checksum": md5 digest
        }

    :param archive: Path to the extracted archive
    :param extract_path: Path to the dir where the archive was extracted
    :returns: A list of checksum dicts
    """
    if tarfile.is_tarfile(archive):
        with tarfile.open(archive) as tarf:
            files = [member.name for member in tarf]
    else:
        with zipfile.ZipFile(archive) as zipf:
            files = [member.filename for member in zipf.infolist()]

    checksums = []
    for _file in files:
        fpath = os.path.abspath(os.path.join(extract_path, _file))
        if os.path.isfile(fpath):
            checksums.append({
                "_id": fpath,
                "checksum": get_file_checksum("md5", fpath)
            })

    return checksums


@api_background_job
def store_archive(project_id, fpath, dir_path, task_id):
    """Extract archive and store files.

    :param str project_id: project ID of the extraction task
    :param str fpath: file path of the archive
    :param str dir_path: directory to where the archive will be
                         extracted, relative to project directory
    :param str task_id: mongo dentifier of the task
    """
    database = db.Database()

    project_dir = db.Projects.get_project_directory(project_id)
    abs_dir_path = project_dir / safe_join("", dir_path)

    lock_manager = ProjectLockManager()

    database.tasks.update_message(
        task_id, "Extracting archive"
    )
    try:
        try:
            extract(fpath, abs_dir_path)
        except (MemberNameError, MemberTypeError, MemberOverwriteError,
                ExtractError) as error:
            # Remove the archive and set task's state
            os.remove(fpath)
            raise ClientError(str(error)) from error

        # Add checksums of the extracted files to mongo
        database.files.insert(_get_archive_checksums(fpath, abs_dir_path))

        # Remove archive and all created symlinks
        os.remove(fpath)
        _process_extracted_files(abs_dir_path)

        # Create metadata
        _post_metadata(dir_path, project_id)
    except Exception:
        lock_manager.release(project_id, abs_dir_path)
        raise

    lock_manager.release(project_id, abs_dir_path)
    return "Archive uploaded and extracted"


def _post_metadata(path, project_id):
    """Create file metadata in Metax.

    This function creates the metadata in Metax for the file or
    directory denoted by path argument.

    :param str path: relative path to file/directory
    :param str project_id: project identifier
    """
    root_upload_path = CONFIG["UPLOAD_PROJECTS_PATH"]

    metax_client = gen_metadata.MetaxClient()

    fpath = db.Projects.get_upload_path(project_id, path)
    return_path = db.Projects.get_return_path(project_id, fpath)

    # POST metadata of all files under dir fpath
    fpaths = []
    for dirpath, _, files in os.walk(fpath):
        for fname in files:
            fpaths.append(os.path.join(dirpath, fname))

    try:
        metax_client.post_metadata(fpaths, root_upload_path, project_id)
    except ResourceAlreadyExistsError as error:
        try:
            failed_files = [file_['object']['file_path']
                            for file_ in error.response.json()['failed']]
        except KeyError:
            # Most likely only one file was posted so Metax response
            # format is different
            failed_files = [return_path]
        raise ClientError(error.message, files=failed_files) from error


@api_background_job
def store_file(project_id, tmp_path, path, task_id):
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

    # Extract files to temporary path
    tmp_dir = pathlib.Path(CONFIG["UPLOAD_TMP_PATH"]) / str(uuid.uuid4())
    (tmp_dir / path).parent.mkdir(parents=True)
    # TODO: extract tmp_path if it is an archive
    shutil.move(tmp_path, tmp_dir / path)

    fpath = db.Projects.get_upload_path(project_id, path)
    return_path = db.Projects.get_return_path(project_id, fpath)

    database.tasks.update_message(
        task_id, f"Creating metadata: {return_path}"
    )

    try:
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
                failed_files = [return_path]
            raise ClientError(error.message, files=failed_files) from error

        # Move file to storage
        storage_path = db.Projects.get_upload_path(project_id, path)
        storage_path.parent.mkdir(exist_ok=True, parents=True)
        shutil.move(tmp_dir / path, storage_path)

        # TODO: Write permission for group is required by packaging
        # service (see https://jira.ci.csc.fi/browse/TPASPKT-516)
        os.chmod(storage_path, 0o664)

        # Remove temporary directory
        for parent in pathlib.Path(path).parents:
            print(parent)
            print(tmp_dir / parent)
            (tmp_dir / parent).rmdir()

        # Update quota
        database.projects.update_used_quota(
            project_id, CONFIG["UPLOAD_PROJECTS_PATH"]
        )
    finally:
        lock_manager = ProjectLockManager()
        lock_manager.release(project_id, fpath)

    return f"Metadata created: {return_path}"
