"""Module for handling the file uploads."""
import os
import pathlib
import tarfile
import uuid
import zipfile

from flask import current_app
import werkzeug

import upload_rest_api.gen_metadata as gen_metadata
import upload_rest_api.utils as utils
from upload_rest_api.api.v1.tasks import TASK_STATUS_API_V1
from upload_rest_api.jobs.utils import UPLOAD_QUEUE, enqueue_background_job

SUPPORTED_TYPES = ("application/octet-stream",)


def _check_extraction_size(user, archive_path):
    """Check whether extracting the archive exceeds users quota.

    :returns: Tuple (quota, used_quota, extracted_size)
    """
    quota = user.get_quota()
    used_quota = user.get_used_quota()

    if tarfile.is_tarfile(archive_path):
        with tarfile.open(archive_path) as archive:
            size = sum(memb.size for memb in archive)
    else:
        with zipfile.ZipFile(archive_path) as archive:
            size = sum(memb.file_size for memb in archive.filelist)

    return quota, used_quota, size


def _save_stream(fpath, stream, checksum, chunk_size=1024*1024):
    """Save the file from request stream.

    Request content is saved to file by reading the stream in chunks of
    chunk_size bytes. If checksum is provided, MD5 sum of file is
    compared to provided MD5 sum. Raises error if checksums do not
    match.

    :param fpath: file path
    :param stream: HTTP request stream
    :param checksum: MD5 checksum of file
    :returns: ``None``
    """
    with open(fpath, "wb") as f_out:
        while True:
            chunk = stream.read(chunk_size)
            if chunk == b'':
                break
            f_out.write(chunk)

    # Verify integrity of uploaded file if checksum was provided
    if checksum and checksum != gen_metadata.md5_digest(fpath):
        os.remove(fpath)
        raise werkzeug.exceptions.BadRequest(
            'Checksum of uploaded file does not match provided checksum.'
        )

    os.chmod(fpath, 0o664)


def save_file(database, user, stream, checksum, upload_path):
    """Save the posted file on disk.

    :param database: Database object
    :param user: User object
    :param stream: HTTP request stream
    :param checksum: MD5 checksum of file, or ``None`` if unknown
    :param upload_path: Upload path, relative to project directory
    :returns: MD5 checksum for file (generated from file)
    """
    file_path = user.project_directory / upload_path

    # Write the file if it does not exist already
    file_path.parent.mkdir(parents=True, exist_ok=True)
    if not file_path.exists():
        _save_stream(file_path, stream, checksum)
    else:
        raise werkzeug.exceptions.Conflict("File already exists")

    md5 = save_file_into_db(
        file_path=file_path,
        database=database,
        user=user
    )
    return md5


def save_file_into_db(file_path, database, user):
    """
    Save the file metadata into the database. This assumes the file has been
    placed into its final location.

    :param str file_path: Path to the file
    :param database: upload_rest_api database instance
    :param user: User database instance

    :returns: MD5 checksum of the file
    :rtype: str
    """
    # Add file checksum to mongo
    md5 = gen_metadata.md5_digest(file_path)
    database.checksums.insert_one(str(file_path.resolve()), md5)

    # Update quota
    user.update_used_quota(current_app.config.get("UPLOAD_PATH"))

    return md5


def save_archive(user, stream, checksum, upload_path):
    """Save archive on disk and enqueue extraction job.

    Archive is saved to file by reading the upload stream in 1MB
    chunks. Archive file is extracted and it is ensured that no symlinks
    are created.

    :param user: User object
    :param stream: HTTP request stream
    :param checksum: MD5 checksum of file, or ``None`` if unknown
    :param upload_path: upload directory, relative to project directory
    :returns: Url of archive extraction task
    """
    dir_path = user.project_directory / upload_path

    if dir_path.is_dir() and not dir_path.samefile(user.project_directory):
        raise werkzeug.exceptions.Conflict(
            f"Directory '{upload_path}' already exists"
        )

    # Save stream to temporary file
    tmp_path = pathlib.Path(current_app.config.get("UPLOAD_TMP_PATH"))
    fpath = tmp_path / str(uuid.uuid4())
    fpath.parent.mkdir(exist_ok=True)
    _save_stream(fpath, stream, checksum)

    # If zip or tar file was uploaded, extract all files
    if zipfile.is_zipfile(fpath) or tarfile.is_tarfile(fpath):
        # Check the uncompressed size
        quota, used_quota, extracted_size = _check_extraction_size(user, fpath)
        if quota - used_quota - extracted_size < 0:
            # Remove the archive and raise an exception
            os.remove(fpath)
            raise werkzeug.exceptions.RequestEntityTooLarge("Quota exceeded")

        user.set_used_quota(used_quota + extracted_size)
        task_id = enqueue_background_job(
            task_func="upload_rest_api.jobs.upload.extract_task",
            queue_name=UPLOAD_QUEUE,
            username=user.username,
            job_kwargs={
                "fpath": fpath,
                "dir_path": dir_path
            }
        )
    else:
        os.remove(fpath)
        raise werkzeug.exceptions.BadRequest(
            "Uploaded file is not a supported archive"
        )

    return utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)


def validate_upload(user, content_length, content_type):
    """Validate the upload request.

    Raises error if upload request is not valid.

    :param user: User object
    :param content_length: Content length of HTTP request
    :param content_type: Content type of HTTP request
    :returns: `None`
    """
    # Check that Content-Length header is provided and uploaded file is
    # not too large
    if content_length is None:
        raise werkzeug.exceptions.LengthRequired(
            "Missing Content-Length header"
        )
    if content_length > current_app.config.get("MAX_CONTENT_LENGTH"):
        raise werkzeug.exceptions.RequestEntityTooLarge(
            "Max single file size exceeded"
        )

    # Check whether the request exceeds users quota. Update used quota
    # first, since multiple users might by using the same project
    user.update_used_quota(current_app.config.get("UPLOAD_PATH"))
    remaining_quota = user.get_quota() - user.get_used_quota()
    if remaining_quota - content_length < 0:
        raise werkzeug.exceptions.RequestEntityTooLarge("Quota exceeded")

    # Check that Content-Type is supported if the header is provided
    if content_type and content_type not in SUPPORTED_TYPES:
        raise werkzeug.exceptions.UnsupportedMediaType(
            f"Unsupported Content-Type: {content_type}"
        )
