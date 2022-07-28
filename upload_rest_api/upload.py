"""Module for handling the file uploads."""
from datetime import datetime, timezone
import os
import shutil
import pathlib
import tarfile
import uuid
import zipfile

from archive_helpers.extract import (ExtractError, MemberNameError,
                                     MemberOverwriteError, MemberTypeError,
                                     extract)
import magic
from metax_access import ResourceAlreadyExistsError
import werkzeug

from upload_rest_api.jobs.utils import ClientError
from upload_rest_api.config import CONFIG
from upload_rest_api import gen_metadata
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
    """Class for handling uploads."""

    def __init__(self, project_id, path, upload_id=None):
        """Initialize upload.

        :param project_id: project identifier
        :param path: upload path
        :param upload_id: Unique identifier of upload
        """
        self.database = Database()
        self.project_id = project_id
        self.path = path

        if upload_id:
            # Continuing previously started upload
            self.upload_id = upload_id
        else:
            # Starting new upload
            self.upload_id = str(uuid.uuid4())
            lock_manager = ProjectLockManager()
            lock_manager.acquire(self.project_id, self.target_path)

        self.tmp_path \
            = pathlib.Path(CONFIG["UPLOAD_TMP_PATH"]) / self.upload_id
        self.tmp_path.mkdir(exist_ok=True, parents=True)

    @property
    def source_path(self):
        """Path to the uploaded file/archive that will be stored."""
        return self.tmp_path / "source"

    @property
    def tmp_project_directory(self):
        """Path to temporary project directory.

        The extracted files are stored here until they can be moved
        to project directory.
        """
        return self.tmp_path / "tmp_storage"

    @property
    def project_directory(self):
        """Path to project directory."""
        return self.database.projects.get_project_directory(self.project_id)

    @property
    def target_path(self):
        """Absolute physical path of upload."""
        return self.project_directory / self.path

    def save_stream(self, stream, checksum):
        """Save the file from stream and verify checksum.

        Save stream to file. If checksum is provided, MD5 sum of file is
        compared to provided MD5 sum. Raises error if checksums do not
        match.

        :param stream: File stream
        :param checksum: MD5 checksum of file, or ``None`` if unknown
        :returns: ``None``
        """
        try:
            # Save stream to temporary file in 1MB chunks
            with open(self.source_path, "wb") as source_file:
                while True:
                    chunk = stream.read(1024*1024)
                    if chunk == b'':
                        break
                    source_file.write(chunk)

            # Verify integrity of uploaded file if checksum was provided
            if checksum \
                    and checksum != get_file_checksum("md5", self.source_path):
                os.remove(self.source_path)
                raise werkzeug.exceptions.BadRequest(
                    'Checksum of uploaded file does not match provided '
                    'checksum.'
                )

        except Exception:
            lock_manager = ProjectLockManager()
            lock_manager.release(self.project_id, self.target_path)
            raise

    def store(self, file_type="file"):
        """Enqueue store task for upload.

        :returns: Url of archive extraction task
        """
        try:
            task_id = enqueue_background_job(
                task_func="upload_rest_api.jobs.upload.store_files",
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
            lock_manager.release(self.project_id, self.target_path)
            raise

    def validate(self, content_length, content_type):
        """Validate the upload.

        Raises error if upload is not valid.

        :param content_length: Content length of HTTP request
        :param content_type: Content type of HTTP request
        :returns: `None`
        """
        try:
            if self.target_path.is_dir() and \
                    not self.target_path.samefile(self.project_directory):
                raise werkzeug.exceptions.Conflict(
                    f"Directory '{self.path}' already exists"
                )

            if self.target_path.is_file() and self.target_path.exists():
                raise werkzeug.exceptions.Conflict("File already exists")

            # Check that Content-Length header is provided and uploaded
            # file is not too large
            if content_length is None:
                raise werkzeug.exceptions.LengthRequired(
                    "Missing Content-Length header"
                )
            if content_length > CONFIG["MAX_CONTENT_LENGTH"]:
                raise werkzeug.exceptions.RequestEntityTooLarge(
                    "Max single file size exceeded"
                )

            # Check whether the request exceeds users quota. Update used
            # quota first, since multiple users might be using the same
            # project
            self.database.projects.update_used_quota(
                self.project_id, CONFIG["UPLOAD_PROJECTS_PATH"]
            )
            project = self.database.projects.get(self.project_id)
            remaining_quota = project["quota"] - project["used_quota"]
            if remaining_quota - content_length < 0:
                raise werkzeug.exceptions.RequestEntityTooLarge(
                    "Quota exceeded"
                )

            # Check that Content-Type is supported if the header is
            # provided
            if content_type and content_type not in SUPPORTED_TYPES:
                raise werkzeug.exceptions.UnsupportedMediaType(
                    f"Unsupported Content-Type: {content_type}"
                )
        except Exception:
            lock_manager = ProjectLockManager()
            lock_manager.release(self.project_id, self.target_path)
            raise

    def validate_archive(self):
        """Validate archive.

        Check that archive is supported format and that the project has
        enough quota.
        """
        try:
            # Ensure that arhive is supported format
            if not (zipfile.is_zipfile(self.source_path)
                    or tarfile.is_tarfile(self.source_path)):
                os.remove(self.source_path)
                raise werkzeug.exceptions.BadRequest(
                    "Uploaded file is not a supported archive"
                )

            # Ensure that the project has enough quota available
            project = self.database.projects.get(self.project_id)
            extracted_size = _extracted_size(self.source_path)
            if project['quota'] - project['used_quota'] - extracted_size < 0:
                # Remove the archive and raise an exception
                os.remove(self.source_path)
                raise werkzeug.exceptions.RequestEntityTooLarge(
                    "Quota exceeded"
                )

            # Update used quota
            self.database.projects.set_used_quota(
                self.project_id, project['used_quota'] + extracted_size
            )
        except Exception:
            lock_manager = ProjectLockManager()
            lock_manager.release(self.project_id, self.target_path)
            raise

    def extract_archive(self):
        """Extract archive to temporary project directory."""
        try:
            # Extract files to temporary project directory
            (self.tmp_project_directory / self.path).parent.mkdir(
                parents=True, exist_ok=True
            )
            try:
                extract(self.source_path,
                        self.tmp_project_directory / self.path)
            except (MemberNameError, MemberTypeError, MemberOverwriteError,
                    ExtractError) as error:
                # Remove the archive and set task's state
                os.remove(self.source_path)
                raise ClientError(str(error)) from error

            # Remove archive
            os.remove(self.source_path)
        except Exception:
            lock_manager = ProjectLockManager()
            lock_manager.release(self.project_id, self.target_path)
            raise

    def store_files(self):
        """Store files.

        Creates metadata for files in temporary project directory, and
        then moves the files to project directory.
        """
        try:
            metadata_dicts = []
            for dirpath, _, files in os.walk(self.tmp_project_directory):
                for fname in files:

                    # Remove symbolic links
                    _file = os.path.join(dirpath, fname)
                    if os.path.islink(_file):
                        os.unlink(_file)
                        continue

                    relative_path = pathlib.Path(_file).relative_to(
                        self.tmp_project_directory
                    )

                    target_path = self.project_directory / relative_path
                    if target_path.exists():
                        raise ClientError(f"File '{relative_path}' already "
                                          "exists", files=[str(relative_path)])

                    # Create metadata
                    timestamp = iso8601_timestamp(_file)
                    metadata_dicts.append({
                        "identifier": str(uuid.uuid4().urn),
                        "file_name": str(os.path.split(_file)[1]),
                        "file_format": _get_mimetype(_file),
                        "byte_size": os.stat(_file).st_size,
                        "file_path": str(relative_path),
                        "project_identifier": self.project_id,
                        "file_uploaded": timestamp,
                        "file_modified": timestamp,
                        "file_frozen": timestamp,
                        "checksum": {
                            "algorithm": "MD5",
                            "value": get_file_checksum("md5", _file),
                            "checked": _timestamp_now()
                        },
                        "file_storage": CONFIG["STORAGE_ID"]
                    })

            # Store basic metadata in database
            self.database.files.insert(
                [
                    {"path": str(self.project_directory / f['file_path']),
                     "checksum": f['checksum']['value'],
                     "identifier": f['identifier']}
                    for f in metadata_dicts
                ]
            )

            # Post all metadata to Metax
            try:
                gen_metadata.MetaxClient().post_metadata(metadata_dicts)
            except ResourceAlreadyExistsError as error:
                try:
                    failed_files = [file_['object']['file_path']
                                    for file_
                                    in error.response.json()['failed']]
                except KeyError:
                    # Most likely only one file was posted so Metax
                    # response format is different
                    failed_files = [self.path]
                raise ClientError(error.message, files=failed_files) from error

            # Move files to project directory
            self._move_files_to_project_directory()

            # Remove temporary directory. The directory might contain
            # empty directories, it must be removed recursively.
            shutil.rmtree(self.tmp_path)

            # Update quota
            self.database.projects.update_used_quota(
                self.project_id, CONFIG["UPLOAD_PROJECTS_PATH"]
            )
        finally:
            lock_manager = ProjectLockManager()
            lock_manager.release(self.project_id, self.target_path)

    def _move_files_to_project_directory(self):
        """Move files to project directory."""
        for dirpath, _, files in os.walk(self.tmp_project_directory):
            for fname in files:
                _file = os.path.join(dirpath, fname)
                relative_path = pathlib.Path(_file).relative_to(
                    self.tmp_project_directory
                )
                source_path = self.tmp_project_directory / relative_path
                target_path = self.project_directory / relative_path
                target_path.parent.mkdir(exist_ok=True, parents=True)
                source_path.rename(target_path)

                # TODO: Write permission for group is required by
                # packaging service
                # (see https://jira.ci.csc.fi/browse/TPASPKT-516)
                os.chmod(target_path, 0o664)


def iso8601_timestamp(fpath):
    """Return last access time in ISO 8601 format."""
    timestamp = datetime.fromtimestamp(
        os.stat(fpath).st_atime, tz=timezone.utc
    ).replace(microsecond=0)
    return timestamp.replace(microsecond=0).isoformat()


def _timestamp_now():
    """Return current time in ISO 8601 format."""
    timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    return timestamp.isoformat()


def _get_mimetype(fpath):
    """Return the MIME type of file fpath."""
    _magic = magic.open(magic.MAGIC_MIME_TYPE)
    _magic.load()
    mimetype = _magic.file(fpath)
    _magic.close()

    return mimetype
