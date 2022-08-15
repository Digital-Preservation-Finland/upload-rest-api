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
import werkzeug

from upload_rest_api.jobs.utils import ClientError
from upload_rest_api.config import CONFIG
from upload_rest_api import gen_metadata
from upload_rest_api.checksum import get_file_checksum
from upload_rest_api.database import Database
from upload_rest_api.jobs.utils import UPLOAD_QUEUE, enqueue_background_job
from upload_rest_api.lock import ProjectLockManager

SUPPORTED_TYPES = ("application/octet-stream",)


def release_lock_on_exception(method):
    """Add file storage lock release functionality to method.

    Returns a decorated method of Upload object. The decorated method
    will release the file storage lock if it fails for any reason.
    """
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)

        except Exception:
            lock_manager = ProjectLockManager()
            lock_manager.release(self.project_id, self.target_path)
            raise

    return wrapper


class UploadConflict(Exception):
    """Exception raised when upload would overwrite existing files."""

    def __init__(self, message, files):
        """Initialize exception.

        :param message: Error message
        :param files: List of conflicting files.
        """
        super().__init__()
        self.message = message
        self.files = files


class Upload:
    """Class for handling uploads."""

    def __init__(self, project_id, path, upload_type="file", upload_id=None):
        """Initialize upload.

        :param project_id: project identifier
        :param path: upload path
        :param upload_type: Type of upload ("file" or "archive")
        :param upload_id: Unique identifier of upload
        """
        self.database = Database()
        self.project_id = project_id
        self.path = path
        self.type = upload_type

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

    @release_lock_on_exception
    def save_stream(self, stream, checksum):
        """Save the file from stream and verify checksum.

        Save stream to file. If checksum is provided, MD5 sum of file is
        compared to provided MD5 sum. Raises error if checksums do not
        match.

        :param stream: File stream
        :param checksum: MD5 checksum of file, or ``None`` if unknown
        :returns: ``None``
        """
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
            self.source_path.unlink()
            raise werkzeug.exceptions.BadRequest(
                'Checksum of uploaded file does not match provided checksum.'
            )

    @release_lock_on_exception
    def enqueue_store_task(self):
        """Enqueue store task for upload.

        :returns: Task identifier
        """
        task_id = enqueue_background_job(
            task_func="upload_rest_api.jobs.upload.store_files",
            queue_name=UPLOAD_QUEUE,
            project_id=self.project_id,
            job_kwargs={
                "project_id": self.project_id,
                "path": self.path,
                "upload_type": self.type,
                "upload_id": self.upload_id
            }
        )

        return task_id

    @release_lock_on_exception
    def validate(self, content_length, content_type):
        """Validate the upload.

        Raises error if upload is not valid.

        :param content_length: Content length of HTTP request
        :param content_type: Content type of HTTP request
        :returns: `None`
        """
        if self.target_path.is_file():
            raise UploadConflict(
                f"File '/{self.path}' already exists", [self.path]
            )

        if self.type == "file" and self.target_path.is_dir():
            raise UploadConflict(
                f"Directory '/{self.path}' already exists", [self.path]
            )

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
            raise werkzeug.exceptions.RequestEntityTooLarge("Quota exceeded")

        # Check that Content-Type is supported if the header is
        # provided
        if content_type and content_type not in SUPPORTED_TYPES:
            raise werkzeug.exceptions.UnsupportedMediaType(
                f"Unsupported Content-Type: {content_type}"
            )

    @release_lock_on_exception
    def validate_archive(self):
        """Validate archive.

        Check that archive is supported format, the project has
        enough quota, and archive does not overwrite existing files.
        """
        # Ensure that arhive is supported format
        if not (zipfile.is_zipfile(self.source_path)
                or tarfile.is_tarfile(self.source_path)):
            self.source_path.unlink()
            raise werkzeug.exceptions.BadRequest(
                "Uploaded file is not a supported archive"
            )

        # Ensure that the project has enough quota available
        project = self.database.projects.get(self.project_id)

        if tarfile.is_tarfile(self.source_path):
            with tarfile.open(self.source_path) as archive:
                extracted_size = sum(member.size for member in archive)
                files = [member.name for member in archive if member.isfile()]
                directories = [member.name for member in archive
                               if member.isdir()]
        else:
            with zipfile.ZipFile(self.source_path) as archive:
                extracted_size = sum(member.file_size
                                     for member in archive.filelist)
                files = [member.filename for member
                         in archive.infolist() if not member.is_dir()]
                directories = [member.filename for member in
                               archive.infolist() if member.is_dir()]

        # Check that files in archive does not overwrite existing
        # files or directories, and that directories in archive do
        # not overwrite files.
        conflicts = []
        for file in files:
            extract_path = self.project_directory / self.path / file
            if extract_path.exists():
                conflicts.append(f'{self.path}/{file}')
        for directory in directories:
            extract_path = self.project_directory / self.path / directory
            if extract_path.is_file():
                conflicts.append(f'{self.path}/{directory}')
        if conflicts:
            self.source_path.unlink()
            raise UploadConflict('Some files already exist', files=conflicts)

        if project['quota'] - project['used_quota'] - extracted_size < 0:
            # Remove the archive and raise an exception
            self.source_path.unlink()
            raise werkzeug.exceptions.RequestEntityTooLarge("Quota exceeded")

        # Update used quota
        self.database.projects.set_used_quota(
            self.project_id, project['used_quota'] + extracted_size
        )

    @release_lock_on_exception
    def extract_archive(self):
        """Extract archive to temporary project directory."""
        # Extract files to temporary project directory
        (self.tmp_project_directory / self.path).parent.mkdir(parents=True,
                                                              exist_ok=True)
        try:
            extract(self.source_path, self.tmp_project_directory / self.path)
        except (MemberNameError, MemberTypeError, MemberOverwriteError,
                ExtractError) as error:
            # Remove the archive and set task's state
            self.source_path.unlink()
            raise ClientError(str(error)) from error

        # Remove archive
        self.source_path.unlink()

        # Remove symbolic links
        for dirpath, _, files in os.walk(self.tmp_project_directory):
            for fname in files:
                file = pathlib.Path(dirpath, fname)
                if file.is_symlink():
                    file.unlink()
                    continue

    @release_lock_on_exception
    def store_files(self):
        """Store files.

        Creates metadata for files in temporary project directory, and
        then moves the files to project directory.
        """
        metax = gen_metadata.MetaxClient()

        # Refuse to store files if Metax has conflicting files. See
        # https://jira.ci.csc.fi/browse/TPASPKT-749 for more
        # information.
        old_metax_files = metax.get_files_dict(self.project_id).keys()
        conflicts = []  # Uploaded files that already exist in Metax
        for dirpath, _, files in os.walk(self.tmp_project_directory):
            for fname in files:
                file = pathlib.Path(dirpath, fname)
                relative_path = file.relative_to(self.tmp_project_directory)

                if f"/{relative_path}" in old_metax_files:
                    conflicts.append(str(relative_path))
                    continue
        if conflicts:
            shutil.rmtree(self.tmp_path)
            raise ClientError('Metadata could not be created because some '
                              'files already have metadata', files=conflicts)

        # Generate metadata
        metadata_dicts = []  # File metadata for Metax
        file_documents = []  # Basic file information to database
        for dirpath, _, files in os.walk(self.tmp_project_directory):
            for fname in files:
                file = pathlib.Path(dirpath, fname)
                relative_path = file.relative_to(self.tmp_project_directory)

                # Create file information for database
                identifier = str(uuid.uuid4().urn)
                checksum = get_file_checksum("md5", file)
                file_documents.append({
                    "path": str(self.project_directory / relative_path),
                    "checksum": checksum,
                    "identifier": identifier
                })

                # Create metadata
                timestamp = iso8601_timestamp(file)
                metadata_dicts.append({
                    "identifier": identifier,
                    "file_name": file.name,
                    "file_format": _get_mimetype(file),
                    "byte_size": file.stat().st_size,
                    "file_path": f"/{relative_path}",
                    "project_identifier": self.project_id,
                    "file_uploaded": timestamp,
                    "file_modified": timestamp,
                    "file_frozen": timestamp,
                    "checksum": {
                        "algorithm": "MD5",
                        "value": checksum,
                        "checked": _timestamp_now()
                    },
                    "file_storage": CONFIG["STORAGE_ID"]
                })

        # Post all metadata to Metax in one go
        metax.post_metadata(metadata_dicts)

        # Insert information of all files to dababase in one go
        self.database.files.insert(file_documents)

        # Move files to project directory
        self._move_files_to_project_directory()

        # Remove temporary directory. The directory might contain
        # empty directories, it must be removed recursively.
        shutil.rmtree(self.tmp_path)

        # Update quota
        self.database.projects.update_used_quota(
            self.project_id, CONFIG["UPLOAD_PROJECTS_PATH"]
        )

        # Release file storage lock
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
                try:
                    source_path.rename(target_path)
                except FileNotFoundError:
                    target_path.parent.mkdir(exist_ok=True, parents=True)
                    source_path.rename(target_path)

                # TODO: Write permission for group is required by
                # packaging service
                # (see https://jira.ci.csc.fi/browse/TPASPKT-516)
                os.chmod(target_path, 0o664)


def iso8601_timestamp(fpath):
    """Return last access time in ISO 8601 format."""
    timestamp = datetime.fromtimestamp(
        fpath.stat().st_atime, tz=timezone.utc
    ).replace(microsecond=0)
    return timestamp.replace(microsecond=0).isoformat()


def _timestamp_now():
    """Return current time in ISO 8601 format."""
    timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    return timestamp.isoformat()


def _get_mimetype(fpath):
    """Return the MIME type of file fpath."""
    try:
        magic_ = magic.open(magic.MAGIC_MIME_TYPE)
        magic_.load()
        mimetype = magic_.file(fpath)
    finally:
        magic_.close()

    return mimetype
