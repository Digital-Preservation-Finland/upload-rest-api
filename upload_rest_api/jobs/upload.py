"""Upload module background jobs."""
import os.path
import tarfile
import zipfile

from archive_helpers.extract import (ExtractError, MemberNameError,
                                     MemberOverwriteError, MemberTypeError,
                                     extract)
from flask import safe_join

import upload_rest_api.database as db
from upload_rest_api.checksum import get_file_checksum
from upload_rest_api.jobs.utils import (METADATA_QUEUE, ClientError,
                                        api_background_job,
                                        enqueue_background_job)
from upload_rest_api.lock import ProjectLockManager


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
def extract_task(project_id, fpath, dir_path, create_metadata, task_id):
    """Calculate the checksum of the archive and extracts the files into
    ``dir_path`` directory.

    :param str project_id: project ID of the extraction task
    :param str fpath: file path of the archive
    :param str dir_path: directory to where the archive will be
                         extracted, relative to project directory
    :param bool create_metadata: create Metax metadata after extraction
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
    except Exception:
        lock_manager.release(project_id, abs_dir_path)
        raise

    if create_metadata:
        try:
            task_id = enqueue_background_job(
                task_func="upload_rest_api.jobs.metadata.post_metadata",
                queue_name=METADATA_QUEUE,
                project_id=project_id,
                job_kwargs={
                    "path": dir_path,
                    "project_id": project_id
                }
            )
        except Exception:
            lock_manager.release(project_id, abs_dir_path)
            raise
    else:
        # We're not doing metadata generation, so release the lock
        # already
        lock_manager.release(project_id, abs_dir_path)

    return "Archive uploaded and extracted"
