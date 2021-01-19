"""Module for handling the file uploads"""
from __future__ import unicode_literals

import json
import logging
import os
import tarfile
import zipfile

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as gen_metadata
import upload_rest_api.utils as utils
from archive_helpers.extract import (MemberNameError, MemberOverwriteError,
                                     MemberTypeError, extract)
from flask import current_app, jsonify, request, safe_join, url_for
from upload_rest_api.api.v1.tasks import TASK_STATUS_API_V1
from upload_rest_api.jobs.utils import UPLOAD_QUEUE, enqueue_background_job

SUPPORTED_TYPES = ("application/octet-stream",)


def _request_exceeds_quota(database):
    """Check whether the request exceeds users quota

    :returns: True if the request exceeds user's quota else False
    """
    username = request.authorization.username
    user = database.user(username)
    quota = user.get_quota() - user.get_used_quota()

    return quota - request.content_length < 0


def _check_extraction_size(database, archive_path, username):
    """Check whether extracting the archive exceeds users quota.

    :returns: Tuple (quota, used_quota, extracted_size)
    """
    user = database.user(username)
    quota = user.get_quota()
    used_quota = user.get_used_quota()

    if tarfile.is_tarfile(archive_path):
        with tarfile.open(archive_path) as archive:
            size = sum(memb.size for memb in archive)
    else:
        with zipfile.ZipFile(archive_path) as archive:
            size = sum(memb.file_size for memb in archive.filelist)

    return quota, used_quota, size


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


def _save_stream(fpath, chunk_size=1024*1024):
    """Save the file into fpath by reading the stream in chunks
    of chunk_size bytes.
    """
    with open(fpath, "wb") as f_out:
        while True:
            chunk = request.stream.read(chunk_size)
            if chunk == b'':
                break
            f_out.write(chunk)

    os.chmod(fpath, 0o664)


class OverwriteError(Exception):
    """Exception for trying to overwrite a existing file"""
    pass


class QuotaError(Exception):
    """Exception for exceeding to quota"""
    pass


class UploadPendingError(Exception):
    """Exception for a pending upload"""
    pass


def _extract(fpath, dir_path, task_id):
    """Extract an archive"""
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


@utils.run_background
def extract_task(fpath, dir_path, task_id=None):
    """This function calculates the checksum of the archive and extracts the
    files into ``dir_path`` directory. Finally updates the status of the task
    into database.

    :param str fpath: file path of the archive
    :param str dir_path: directory to where the archive will be extracted
    :param str task_id: mongo dentifier of the task

    :returns: The mongo identifier of the task
    """
    try:
        _extract(fpath, dir_path, task_id)
    except Exception as error:
        logging.error(str(error), exc_info=error)
        tasks = db.Database().tasks
        tasks.update_status(task_id, "error")
        tasks.update_message(task_id, "Internal server error")
        raise

    return task_id


def save_file(database, project, fpath):
    """Save the posted file on disk at fpath by reading
    the upload stream in 1MB chunks.

    :param database: Database object
    :param project: File's project
    :param fpath: Path where to save the file
    :returns: HTTP Response
    """
    # Write the file if it does not exist already
    if not os.path.exists(fpath):
        _save_stream(fpath)
    else:
        raise OverwriteError("File already exists")

    # Add file checksum to mongo
    md5 = gen_metadata.md5_digest(fpath)
    database.checksums.insert_one(os.path.abspath(fpath), md5)
    file_path = utils.get_return_path(project, fpath)
    response = jsonify({
        "file_path": file_path,
        "md5": md5,
        "status": "created"
    })
    response.status_code = 200

    return response


def save_archive(database, fpath, upload_dir):
    """Uploads the archive on disk at fpath by reading
    the upload stream in 1MB chunks. Extracts the archive file
    and checks that no symlinks are created.

    :param database: Database object
    :param fpath: Path where to save the file
    :param upload_dir: Directory to which the archive is extracted
    :returns: HTTP Response
    """
    username = request.authorization.username
    project = database.user(username).get_project()
    dir_path = utils.get_project_path(project)
    if upload_dir:
        dir_path = safe_join(dir_path, upload_dir)
        if os.path.isdir(dir_path):
            raise OverwriteError("Directory '%s' already exists" % upload_dir)
        else:
            os.makedirs(dir_path)

    _save_stream(fpath)

    # If zip or tar file was uploaded, extract all files
    if zipfile.is_zipfile(fpath) or tarfile.is_tarfile(fpath):
        # Check the uncompressed size
        quota, used_quota, extracted_size = _check_extraction_size(
            database, fpath, username
        )
        if quota - used_quota - extracted_size < 0:
            # Remove the archive and raise an exception
            os.remove(fpath)
            raise QuotaError("Quota exceeded")

        database.user(username).set_used_quota(used_quota + extracted_size)
        task_id = enqueue_background_job(
            task_func="upload_rest_api.jobs.upload.extract_task",
            queue_name=UPLOAD_QUEUE,
            username=username,
            job_kwargs={
                "fpath": fpath,
                "dir_path": dir_path
            }
        )
        polling_url = utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)
        response = jsonify({
            "file_path": "/",
            "message": "Uploading archive",
            "polling_url": polling_url,
            "status": "pending"
        })
        location = url_for(TASK_STATUS_API_V1.name + ".task_status",
                           task_id=task_id)
        response.headers[b'Location'] = location
        response.status_code = 202
    else:
        os.remove(fpath)
        response = utils.make_response(
            400, "Uploaded file is not a supported archive"
        )

    return response


def validate_upload(database):
    """Validates the upload request

    :returns: `None` if the validation succeeds. Otherwise error response
              if validation failed.
    """
    response = None

    # Update used_quota also at the start of the function
    # since multiple users might by using the same project
    database.user(request.authorization.username).update_used_quota(
        current_app.config.get("UPLOAD_PATH")
    )

    # Check that Content-Length header is provided
    if request.content_length is None:
        response = utils.make_response(400, "Missing Content-Length header")

    # Check that Content-Type is supported if the header is provided
    content_type = request.content_type
    if content_type and content_type not in SUPPORTED_TYPES:
        response = utils.make_response(
            415, "Unsupported Content-Type: %s" % content_type
        )

    # Check user quota
    if request.content_length > current_app.config.get("MAX_CONTENT_LENGTH"):
        response = utils.make_response(413, "Max single file size exceeded")
    elif _request_exceeds_quota(database):
        response = utils.make_response(413, "Quota exceeded")
    return response
