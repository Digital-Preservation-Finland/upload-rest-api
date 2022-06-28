"""Module for handling the file uploads."""
import os
import pathlib
import tarfile
import uuid
import zipfile

import werkzeug
from flask import current_app
from upload_rest_api import utils
from upload_rest_api.api.v1.tasks import TASK_STATUS_API_V1
from upload_rest_api.checksum import get_file_checksum
from upload_rest_api.database import Database, Projects
from upload_rest_api.jobs.utils import UPLOAD_QUEUE, enqueue_background_job
from upload_rest_api.lock import ProjectLockManager

SUPPORTED_TYPES = ("application/octet-stream",)


def _extracted_size(archive_path):
    """Compute the total size of archive content.

    :returns: Size of extracted archive
    """
    if tarfile.is_tarfile(archive_path):
        with tarfile.open(archive_path) as archive:
            size = sum(memb.size for memb in archive)
    else:
        with zipfile.ZipFile(archive_path) as archive:
            size = sum(memb.file_size for memb in archive.filelist)

    return size


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
    if checksum and checksum != get_file_checksum("md5", fpath):
        os.remove(fpath)
        raise werkzeug.exceptions.BadRequest(
            'Checksum of uploaded file does not match provided checksum.'
        )


class Upload:
    """Upload."""

    def __init__(self, project_id, path):
        """Initialize upload."""
        self.database = Database()
        self.project_id = project_id
        self.path = path
        self.tmp_path = pathlib.Path(
            current_app.config.get("UPLOAD_TMP_PATH")
        ) / str(uuid.uuid4())

    @property
    def file_path(self):
        """Absolute physical path of upload."""
        return Projects.get_project_directory(self.project_id) / self.path

    def save_stream(self, stream, checksum):
        """Save archive on disk and enqueue extraction job.

        Archive is saved to file by reading the upload stream in 1MB
        chunks. Archive file is extracted and it is ensured that no
        symlinks are created.

        :param stream: HTTP request stream
        :param checksum: MD5 checksum of file, or ``None`` if unknown
        :returns: Url of archive extraction task
        """
        lock_manager = ProjectLockManager()
        lock_manager.acquire(self.project_id, self.file_path)
        project_dir = Projects.get_project_directory(self.project_id)
        try:
            if self.file_path.is_dir() and \
                    not self.file_path.samefile(project_dir):
                raise werkzeug.exceptions.Conflict(
                    f"Directory '{self.path}' already exists"
                )

            if self.file_path.is_file() and self.file_path.exists():
                raise werkzeug.exceptions.Conflict("File already exists")

            # Save stream to temporary file
            self.tmp_path.parent.mkdir(exist_ok=True)
            _save_stream(self.tmp_path, stream, checksum)

        except Exception:
            lock_manager.release(self.project_id, self.file_path)
            raise

    def save_file_into_db(self, md5=None):
        """Save the file metadata into the database.

        This assumes the file has been placed into its final location.

        :param str md5: Optional precomputed MD5 checksum. If not
                        provided, the checksum will be calculated.

        :returns: MD5 checksum of the file
        :rtype: str
        """
        if not md5:
            md5 = get_file_checksum(algorithm="md5", path=self.tmp_path)

        # Add file checksum to mongo
        self.database.files.insert_one(str(self.file_path.resolve()), md5)

        return md5

    def extract_archive(self):
        """Enqueue extraction job for an existing archive file on disk.

        Archive file is extracted and it is ensured that no symlinks
        are created. The original archive will be deleted upon
        completion.

        :returns: Url of archive extraction task
        """
        try:
            # Ensure that arhive is supported format
            if not (zipfile.is_zipfile(self.tmp_path)
                    or tarfile.is_tarfile(self.tmp_path)):
                os.remove(self.tmp_path)
                raise werkzeug.exceptions.BadRequest(
                    "Uploaded file is not a supported archive"
                )

            # Ensure that the project has enough quota available
            project = self.database.projects.get(self.project_id)
            extracted_size = _extracted_size(self.tmp_path)
            if project['quota'] - project['used_quota'] - extracted_size < 0:
                # Remove the archive and raise an exception
                os.remove(self.tmp_path)
                raise werkzeug.exceptions.RequestEntityTooLarge(
                    "Quota exceeded"
                )

            # Update used quota
            self.database.projects.set_used_quota(
                self.project_id, project['used_quota'] + extracted_size
            )

            task_id = enqueue_background_job(
                task_func="upload_rest_api.jobs.upload.extract_task",
                queue_name=UPLOAD_QUEUE,
                project_id=self.project_id,
                job_kwargs={
                    "project_id": self.project_id,
                    "fpath": self.tmp_path,
                    "dir_path": self.path,
                }
            )

            return utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)
        except Exception:
            lock_manager = ProjectLockManager()
            lock_manager.release(self.project_id, self.file_path)
            raise

    def validate(self, content_length, content_type):
        """Validate the upload request.

        Raises error if upload request is not valid.

        :param content_length: Content length of HTTP request
        :param content_type: Content type of HTTP request
        :returns: `None`
        """
        # Check that Content-Length header is provided and uploaded file
        # is not too large
        if content_length is None:
            raise werkzeug.exceptions.LengthRequired(
                "Missing Content-Length header"
            )
        if content_length > current_app.config.get("MAX_CONTENT_LENGTH"):
            raise werkzeug.exceptions.RequestEntityTooLarge(
                "Max single file size exceeded"
            )

        # Check whether the request exceeds users quota. Update used
        # quota first, since multiple users might be using the same
        # project
        database = Database()
        database.projects.update_used_quota(
            self.project_id, current_app.config.get("UPLOAD_PROJECTS_PATH")
        )
        project = database.projects.get(self.project_id)
        remaining_quota = project["quota"] - project["used_quota"]
        if remaining_quota - content_length < 0:
            raise werkzeug.exceptions.RequestEntityTooLarge("Quota exceeded")

        # Check that Content-Type is supported if the header is provided
        if content_type and content_type not in SUPPORTED_TYPES:
            raise werkzeug.exceptions.UnsupportedMediaType(
                f"Unsupported Content-Type: {content_type}"
            )
