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
        """Save the file from stream and verify checksum.

        Save stream to file. If checksum is provided, MD5 sum of file is
        compared to provided MD5 sum. Raises error if checksums do not
        match.

        :param stream: File stream
        :param checksum: MD5 checksum of file, or ``None`` if unknown
        :returns: ``None``
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

            # Save stream to temporary file in 1MB chunks
            self.tmp_path.parent.mkdir(exist_ok=True)
            with open(self.tmp_path, "wb") as tmp_file:
                while True:
                    chunk = stream.read(1024*1024)
                    if chunk == b'':
                        break
                    tmp_file.write(chunk)

            # Verify integrity of uploaded file if checksum was provided
            if checksum \
                    and checksum != get_file_checksum("md5", self.tmp_path):
                os.remove(self.tmp_path)
                raise werkzeug.exceptions.BadRequest(
                    'Checksum of uploaded file does not match provided '
                    'checksum.'
                )

        except Exception:
            lock_manager.release(self.project_id, self.file_path)
            raise

    def store(self, file_type="file"):
        """Enqueue store task for upload.

        :returns: Url of archive extraction task
        """
        try:
            task_id = enqueue_background_job(
                task_func="upload_rest_api.jobs.upload.store_file",
                queue_name=UPLOAD_QUEUE,
                project_id=self.project_id,
                job_kwargs={
                    "project_id": self.project_id,
                    "tmp_path": self.tmp_path,
                    "path": self.path,
                    "file_type": file_type
                }
            )

            return utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)
        except Exception:
            lock_manager = ProjectLockManager()
            lock_manager.release(self.project_id, self.file_path)
            raise

    def validate(self, content_length, content_type):
        """Validate the upload.

        Raises error if upload is not valid.

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

    def validate_archive(self):
        """Validate archive.

        Check that archive is supported format and that the project has
        enough quota.
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
        except Exception:
            lock_manager = ProjectLockManager()
            lock_manager.release(self.project_id, self.file_path)
            raise
