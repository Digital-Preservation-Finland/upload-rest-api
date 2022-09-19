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
import metax_access

from upload_rest_api.config import CONFIG
from upload_rest_api import gen_metadata
from upload_rest_api.checksum import get_file_checksum
from upload_rest_api.database import Database
from upload_rest_api.jobs.utils import UPLOAD_QUEUE, enqueue_background_job
from upload_rest_api.lock import ProjectLockManager
from upload_rest_api.resource import Resource


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
            lock_manager.release(self.project.identifier,
                                 self.storage_path)
            raise

    return wrapper


class UploadError(Exception):
    """Exception raised when upload fails."""


class UploadConflictError(UploadError):
    """Exception raised when upload would overwrite existing files."""

    def __init__(self, message, files):
        """Initialize exception.

        :param message: Error message
        :param files: List of conflicting files.
        """
        super().__init__()
        self.message = message
        self.files = files


class InvalidArchiveError(UploadError):
    """Exception raised when archive can not be extracted or stored."""


class InsufficientQuotaError(UploadError):
    """Exception raised when upload would exceed remaining quota."""


def create_upload(project_id, path, size, upload_type='file', identifier=None):
    """Create new upload."""
    upload = Upload(project_id, path, upload_type=upload_type,
                    identifier=identifier)
    upload.create(size)
    return upload


def continue_upload(project_id, path, upload_type, identifier):
    """Continue existing upload."""
    upload = Upload(project_id, path, upload_type=upload_type,
                    identifier=identifier)
    return upload


class Upload:
    """Class for handling uploads."""

    def __init__(self, project, path, upload_type="file", identifier=None):
        """Initialize upload.

        :param project_id: project identifier
        :param path: upload path
        :param upload_type: Type of upload ("file" or "archive")
        :param identifier: Identifier of upload
        """
        self.database = Database()
        self._resource = Resource(project, path)
        self.type = upload_type
        self.source_checksum = None
        self.identifier = identifier

    def create(self, size):
        """Create new upload.

        Check that project has enough quota, check for conflicts and
        lock the filestorage.

        :param size: Size of upload
        """
        if not self.identifier:
            self.identifier = str(uuid.uuid4())
        if size > CONFIG["MAX_CONTENT_LENGTH"]:
            raise InsufficientQuotaError("Max single file size exceeded")

        # Check that project has enough quota. Update used quota
        # first, since multiple users might be using the same
        # project
        self.database.projects.update_used_quota(
            self.project.identifier, CONFIG["UPLOAD_PROJECTS_PATH"]
        )
        if self.project.remaining_quota() - size < 0:
            raise InsufficientQuotaError("Quota exceeded")

        # Check for conflicts
        if self.storage_path.is_file():
            raise UploadConflictError(
                f"File '{self.path}' already exists", [str(self.path)]
            )

        if self.type == "file" and self.storage_path.is_dir():
            raise UploadConflictError(
                f"Directory '{self.path}' already exists", [str(self.path)]
            )

        # Lock the storage path
        lock_manager = ProjectLockManager()
        lock_manager.acquire(self.project.identifier, self.storage_path)

        # Create temporary path
        self.tmp_path.mkdir(exist_ok=True, parents=True)

    @property
    def project(self):
        """Project of upload."""
        return self._resource.project

    @property
    def path(self):
        """Upload path."""
        return self._resource.path

    @property
    def storage_path(self):
        """Path where resource will be stored."""
        return self._resource.storage_path

    @property
    def tmp_path(self):
        """Temporary path for upload."""
        return pathlib.Path(CONFIG["UPLOAD_TMP_PATH"]) / self.identifier

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
    def tmp_storage_path(self):
        """Temporary storage path of resource.

        The storage path of resource in temporary project directory.
        """
        return self.tmp_project_directory \
            / self.storage_path.relative_to(self.project.directory)

    @release_lock_on_exception
    def add_source(self, file, checksum, verify=True):
        """Save file to source path and verify checksum.

        Save file to source path. If checksum is provided, MD5 sum of
        file is compared to provided MD5 sum. Raises error if checksums
        do not match.

        :param stream: File stream or path to file
        :param size: Size of file/archive
        :param checksum: MD5 checksum of file, or ``None`` if unknown
        :returns: ``None``
        """
        if 'read' in dir(file):
            # 'file' is a stream. Write it to source path in 1MB chunks
            with open(self.source_path, "wb") as source_file:
                while True:
                    chunk = file.read(1024*1024)
                    if chunk == b'':
                        break
                    source_file.write(chunk)
        else:
            # 'file' is path to a file. Move it to source path.
            pathlib.Path(file).rename(self.source_path)

        # Verify integrity of uploaded file if checksum was provided
        if checksum:
            self.source_checksum = checksum
            if verify and self.source_checksum \
                    != get_file_checksum("md5", self.source_path):
                self.source_path.unlink()
                raise UploadError(
                    'Checksum of uploaded file does not match provided '
                    'checksum.'
                )

    @release_lock_on_exception
    def enqueue_store_task(self):
        """Enqueue store task for upload.

        :returns: Task identifier
        """
        task_id = enqueue_background_job(
            task_func="upload_rest_api.jobs.upload.store_files",
            queue_name=UPLOAD_QUEUE,
            project_id=self.project.identifier,
            job_kwargs={
                "project_id": self.project.identifier,
                "path": str(self.path),
                "upload_type": self.type,
                "identifier": self.identifier
            }
        )

        return task_id

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
            raise UploadError(
                "Uploaded file is not a supported archive"
            )

        # Ensure that the project has enough quota available
        project = self.database.projects.get(self.project.identifier)

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
            extract_path = self.storage_path / file
            if extract_path.exists():
                conflicts.append(f'{self.path}/{file}')
        for directory in directories:
            extract_path = self.storage_path / directory
            if extract_path.is_file():
                conflicts.append(f'{self.path}/{directory}')
        if conflicts:
            self.source_path.unlink()
            raise UploadConflictError('Some files already exist',
                                      files=conflicts)

        if project['quota'] - project['used_quota'] - extracted_size < 0:
            # Remove the archive and raise an exception
            self.source_path.unlink()
            raise InsufficientQuotaError("Quota exceeded")

        # Update used quota
        self.database.projects.set_used_quota(
            self.project.identifier, project['used_quota'] + extracted_size
        )

    def _extract_archive(self):
        """Extract archive to temporary project directory."""
        # Extract files to temporary project directory
        self.tmp_storage_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            extract(self.source_path, self.tmp_storage_path)
        except (MemberNameError, MemberTypeError, MemberOverwriteError,
                ExtractError) as error:
            # Remove the archive and set task's state
            self.source_path.unlink()
            raise InvalidArchiveError(str(error)) from error

        # Remove archive
        self.source_path.unlink()

    @release_lock_on_exception
    def store_files(self):
        """Store files.

        Moves/extracts source files to temporary project directory,
        creates file metadata, and then moves the files to project
        directory.
        """
        if self.type == 'file':
            self.tmp_storage_path.parent.mkdir(parents=True, exist_ok=True)
            self.source_path.rename(self.tmp_storage_path)
        else:
            self._extract_archive()

        # Refuse to store files if Metax has conflicting files. See
        # https://jira.ci.csc.fi/browse/TPASPKT-749 for more
        # information.
        metax = gen_metadata.MetaxClient()
        new_files = []
        for dirpath, _, files in os.walk(self.tmp_project_directory):
            for fname in files:
                new_files.append(
                    pathlib.Path(dirpath, fname).relative_to(
                        self.tmp_project_directory
                    )
                )
        if len(new_files) == 1:
            # Creating metadata for only one file, so it is probably
            # more efficient to retrieve information about single file
            try:
                old_file = metax.client.get_project_file(
                    self.project.identifier,
                    str(self.path)
                )
                shutil.rmtree(self.tmp_path)
                raise UploadConflictError(
                    'Metadata could not be created because the file'
                    ' already has metadata',
                    files=[old_file['file_path']]
                )
            except metax_access.metax.FileNotAvailableError:
                # No conflicts
                pass
        else:
            # Retrieve list of all files as one request to avoid sending
            # too many requests to Metax.
            conflicts = []  # Uploaded files that already exist in Metax
            old_files = metax.get_files_dict(self.project.identifier).keys()
            for file in new_files:
                if f"/{file}" in old_files:
                    conflicts.append(str(file))
            if conflicts:
                shutil.rmtree(self.tmp_path)
                raise UploadConflictError(
                    'Metadata could not be created because some files '
                    'already have metadata', files=conflicts
                )

        # Generate metadata
        metadata_dicts = []  # File metadata for Metax
        file_documents = []  # Basic file information to database
        for dirpath, _, files in os.walk(self.tmp_project_directory):
            for fname in files:
                file = pathlib.Path(dirpath, fname)
                relative_path = file.relative_to(self.tmp_project_directory)

                # Create file information for database
                identifier = str(uuid.uuid4().urn)
                if self.type == 'file' and self.source_checksum:
                    checksum = self.source_checksum
                else:
                    checksum = get_file_checksum("md5", file)
                file_documents.append({
                    "path": str(self.project.directory / relative_path),
                    "checksum": checksum,
                    "identifier": identifier
                })

                # Create metadata
                timestamp = _iso8601_timestamp(file)
                metadata_dicts.append({
                    "identifier": identifier,
                    "file_name": file.name,
                    "file_format": _get_mimetype(file),
                    "byte_size": file.stat().st_size,
                    "file_path": f"/{relative_path}",
                    "project_identifier": self.project.identifier,
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
            self.project.identifier, CONFIG["UPLOAD_PROJECTS_PATH"]
        )

        # Release file storage lock
        lock_manager = ProjectLockManager()
        lock_manager.release(self.project.identifier, self.storage_path)

    def _move_files_to_project_directory(self):
        """Move files to project directory."""
        for dirpath, _, files in os.walk(self.tmp_project_directory):
            for fname in files:
                _file = os.path.join(dirpath, fname)
                relative_path = pathlib.Path(_file).relative_to(
                    self.tmp_project_directory
                )
                source_path = self.tmp_project_directory / relative_path
                target_path = self.project.directory / relative_path
                try:
                    source_path.rename(target_path)
                except FileNotFoundError:
                    target_path.parent.mkdir(exist_ok=True, parents=True)
                    source_path.rename(target_path)

                # TODO: Write permission for group is required by
                # packaging service
                # (see https://jira.ci.csc.fi/browse/TPASPKT-516)
                os.chmod(target_path, 0o664)


def _iso8601_timestamp(fpath):
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
