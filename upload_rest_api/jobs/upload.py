from __future__ import unicode_literals

import json
import logging
import os.path
import tarfile
import zipfile

from requests.exceptions import HTTPError

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as gen_metadata
import upload_rest_api.utils as utils
from archive_helpers.extract import (MemberNameError, MemberOverwriteError,
                                     MemberTypeError, extract)
from metax_access import MetaxError
from upload_rest_api.config import CONFIG
from upload_rest_api.jobs.utils import api_background_job


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
    """Calculate md5 checksums of all archive members and return a list of
    dicts::

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
                "checksum": gen_metadata.md5_digest(fpath)
            })

    return checksums


@api_background_job
def extract_archive(fpath, dir_path, task_id):
    """This RQ job calculates the checksum of the archive and extracts the
    files into ``dir_path`` directory. Finally updates the status of the task
    into database.

    :param str fpath: file path of the archive
    :param str dir_path: directory to where the archive will be extracted
    :param str task_id: mongo dentifier of the task
    """
    database = db.Database()

    database.tasks.update_message(
        task_id, "Extracting archive"
    )
    md5 = gen_metadata.md5_digest(fpath)
    try:
        extract(fpath, dir_path)
    except (MemberNameError, MemberTypeError, MemberOverwriteError) as error:
        logging.error(str(error), exc_info=error)
        # Remove the archive and set task's state
        os.remove(fpath)
        database.tasks.update_status(task_id, "error")
        msg = {"message": str(error)}
        database.tasks.update_message(task_id, json.dumps(msg))
    else:
        # Add checksums of the extracted files to mongo
        database.checksums.insert(_get_archive_checksums(fpath, dir_path))

        # Remove archive and all created symlinks
        os.remove(fpath)
        _process_extracted_files(dir_path)

        database.tasks.update_status(task_id, "done")
        msg = {"message": "Archive uploaded and extracted",
               "md5": md5}
        database.tasks.update_message(task_id, json.dumps(msg))
