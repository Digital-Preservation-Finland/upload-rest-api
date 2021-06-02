"""Module for handling the file uploads."""
import os
import tarfile
import zipfile

from flask import current_app, jsonify, request, safe_join, url_for

import upload_rest_api.gen_metadata as gen_metadata
import upload_rest_api.utils as utils
from upload_rest_api.api.v1.tasks import TASK_STATUS_API_V1
from upload_rest_api.jobs.utils import UPLOAD_QUEUE, enqueue_background_job

SUPPORTED_TYPES = ("application/octet-stream",)


def _request_exceeds_quota(database):
    """Check whether the request exceeds users quota.

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

    # Verify integrity of uploaded file if checksum was provided
    if 'md5' in request.args \
            and request.args['md5'] != gen_metadata.md5_digest(fpath):
        os.remove(fpath)
        raise DataIntegrityError(
            'Checksum of uploaded file does not match provided checksum.'
        )

    os.chmod(fpath, 0o664)


class OverwriteError(Exception):
    """Exception for trying to overwrite a existing file."""


class QuotaError(Exception):
    """Exception for exceeding to quota."""


class UploadPendingError(Exception):
    """Exception for a pending upload."""


class DataIntegrityError(Exception):
    """Exception for data corruption during a transfer."""


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
    """Validate the upload request.

    :returns: `None` if the validation succeeds. Otherwise error
              response if validation failed.
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
