"""Upload model."""
import os
import shutil
import tarfile
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import magic
import metax_access
from archive_helpers.extract import (ExtractError, MemberNameError,
                                     MemberOverwriteError, MemberTypeError,
                                     extract)

from upload_rest_api.metax import get_metax_client
from upload_rest_api.models.project import ProjectEntry, Project
from upload_rest_api.models.file_entry import FileEntry
from upload_rest_api.models.resource import Resource
from upload_rest_api.models.upload_entry import (UploadType, UploadEntry)
from upload_rest_api.checksum import get_file_checksum
from upload_rest_api.config import CONFIG
from upload_rest_api.jobs.utils import (UPLOAD_QUEUE, enqueue_background_job)
from upload_rest_api.lock import ProjectLockManager


def _release_lock_on_exception(method):
    """Add file storage lock release functionality to method.

    Returns a decorated method of Upload object. The decorated method
    will release the file storage lock if it fails for any reason.
    """
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)

        except Exception:
            lock_manager = ProjectLockManager()
            lock_manager.release(self.project.id,
                                 self.storage_path)
            raise

    return wrapper


class UploadError(Exception):
    """Exception raised when upload fails."""

    def __init__(self, message, files=None):
        """Initialize exception.

        :param message: Error message
        :param files: Optional ist of files that caused the error.
        """
        super().__init__(message)
        self.files = files


class UploadConflictError(UploadError):
    """Exception raised when upload would overwrite existing files."""


class InvalidArchiveError(UploadError):
    """Exception raised when archive can not be extracted."""


class InsufficientQuotaError(UploadError):
    """Exception raised when upload would exceed remaining quota."""


class Upload:
    """Class for handling uploads."""
    def __init__(self, db_upload):
        self._db_upload = db_upload
        self._resource = None

    # Read-only properties for database fields
    id = property(lambda x: x._db_upload.id)
    path = property(lambda x: x._db_upload.path)
    type_ = property(lambda x: x._db_upload.type_)
    source_checksum = property(lambda x: x._db_upload.source_checksum)
    size = property(lambda x: x._db_upload.size)
    is_tus_upload = property(lambda x: x._db_upload.is_tus_upload)
    started_at = property(lambda x: x._db_upload.started_at)
    project = property(lambda x: Project(x._db_upload.project))

    DoesNotExist = UploadEntry.DoesNotExist

    @property
    def resource(self):
        """The file or directory to be uploaded."""
        if not self._resource:
            self._resource = Resource(self.project, self.path)

        return self._resource

    @classmethod
    def get(cls, **kwargs):
        """
        Retrieve an existing upload

        :param kwargs: Keyword arguments used to retrieve the upload

        :returns: Upload instance
        """
        return cls(
            db_upload=UploadEntry.objects.get(**kwargs)
        )

    @property
    def storage_path(self):
        """Absolute path to the file on disk"""
        return self.resource.storage_path

    @property
    def _tmp_path(self):
        """Temporary path for upload."""
        return Path(CONFIG["UPLOAD_TMP_PATH"]) / self.id

    @property
    def _tmp_project_directory(self):
        """Path to temporary project directory.

        The extracted files are stored here until they can be moved
        to project directory.
        """
        return self._tmp_path / "tmp_storage"

    @property
    def _tmp_storage_path(self):
        """Temporary storage path of resource.

        The storage path of resource in temporary project directory.
        """
        return self._tmp_project_directory \
            / self.storage_path.relative_to(self.project.directory)

    @property
    def _source_path(self):
        """Path to the source file/archive."""
        return self._tmp_path / "source"

    @classmethod
    def create(
            cls, project_id, path, size, type_=UploadType.FILE,
            identifier=None, is_tus_upload=None):
        """
        Create upload from the given tus resource.

        :param str project_id: Project identifier
        :param str path: Relative file/directory path
        :param str type_: Upload type
        """
        # TODO: Could the resource be provided as argument? The resource
        # would contain project_id, path and type_.
        resource = Resource(Project.get(id=project_id), path)
        if not identifier:
            identifier = str(uuid.uuid4())
        if size > CONFIG["MAX_CONTENT_LENGTH"]:
            raise InsufficientQuotaError("Max single file size exceeded")

        db_upload = UploadEntry(
            id=identifier,
            project=ProjectEntry.objects.get(id=resource.project.id),
            path=str(resource.path),
            type_=type_,
            size=size
        )
        if is_tus_upload is not None:
            db_upload.is_tus_upload = is_tus_upload
        upload = cls(db_upload=db_upload)

        # Check that project has enough quota. Update used quota
        # first, since multiple users might be using the same
        # project
        upload.project.update_used_quota()

        if upload.project.remaining_quota - size < 0:
            raise InsufficientQuotaError("Quota exceeded")

        # Check for conflicts
        if upload.storage_path.is_file():
            raise UploadConflictError(
                f"File '{upload.resource.path}' already exists",
                [str(upload.resource.path)]
            )

        dir_already_exists = (
            upload.type_ == UploadType.FILE
            and upload.storage_path.is_dir()
        )

        if dir_already_exists:
            raise UploadConflictError(
                f"Directory '{upload.resource.path}' already exists",
                [str(upload.resource.path)]
            )

        # Lock the storage path
        lock_manager = ProjectLockManager()
        lock_manager.acquire(upload.project.id, upload.storage_path)

        # Create temporary path
        upload._tmp_path.mkdir(exist_ok=True, parents=True)

        db_upload.save(force_insert=True)

        return upload

    @_release_lock_on_exception
    def add_source(self, file, checksum):
        """Save file to source path.

        :param file: File stream or path to file
        :param checksum: MD5 checksum of file, or ``None`` if unknown
        :returns: ``None``
        """
        if 'read' in dir(file):
            # 'file' is a stream. Write it to source path in 1MB chunks
            with open(self._source_path, "wb") as source_file:
                while True:
                    chunk = file.read(1024*1024)
                    if chunk == b'':
                        break
                    source_file.write(chunk)
        else:
            # 'file' is path to a file. Move it to source path.
            Path(file).rename(self._source_path)

        self._db_upload.source_checksum = checksum

    @_release_lock_on_exception
    def enqueue_store_task(self, verify_source):
        """Enqueue store task for upload.

        :returns: Task identifier
        """
        self._db_upload.save()

        task_id = enqueue_background_job(
            task_func="upload_rest_api.jobs.upload.store_files",
            task_id=self.id,
            queue_name=UPLOAD_QUEUE,
            project_id=self.project.id,
            job_kwargs={
                "identifier": self.id,
                "verify_source": verify_source
            }
        )

        return task_id

    def _extract_archive(self):
        """Extract archive to temporary project directory."""
        # Ensure that arhive is supported format
        if not (zipfile.is_zipfile(self._source_path)
                or tarfile.is_tarfile(self._source_path)):
            self._source_path.unlink()
            raise UploadError(
                "Uploaded file is not a supported archive"
            )

        # Read the content of the archive, and compute the total size of
        # files.
        if tarfile.is_tarfile(self._source_path):
            with tarfile.open(self._source_path) as archive:
                extracted_size = sum(member.size for member in archive)
                files = [member.name for member in archive if member.isfile()]
                directories = [member.name for member in archive
                               if member.isdir()]
        else:
            with zipfile.ZipFile(self._source_path) as archive:
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
            self._source_path.unlink()
            raise UploadConflictError('Some files already exist',
                                      files=conflicts)

        # Ensure that the project has enough quota available
        if self.project.remaining_quota - extracted_size < 0:
            # Remove the archive and raise an exception
            self._source_path.unlink()
            raise InsufficientQuotaError("Quota exceeded")

        # Update used quota to account for the total size of the archive
        # contents.
        self.project.increase_used_quota(extracted_size)

        # Extract files to temporary project directory
        self._tmp_storage_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            extract(self._source_path, self._tmp_storage_path)
        except (MemberNameError, MemberTypeError, MemberOverwriteError,
                ExtractError) as error:
            # Remove the archive and set task's state
            self._source_path.unlink()
            raise InvalidArchiveError(str(error)) from error

        # Remove archive
        self._source_path.unlink()

    @_release_lock_on_exception
    def store_files(self, verify_source):
        """Store files.

        Moves/extracts source files to temporary project directory,
        creates file metadata, and then moves the files to project
        directory.

        :param verify_source: verify integrity of source file
        """
        # Verify integrity of source file if checksum was provided
        # TODO: Can source file verfication be removed from this
        # function when TPASPKT-952 is done?
        if verify_source \
                and self.source_checksum \
                != get_file_checksum("md5", self._source_path):
            self._source_path.unlink()
            raise UploadError(
                'Checksum of uploaded file does not match provided '
                'checksum.'
            )

        if self.type_ == UploadType.FILE:
            self._tmp_storage_path.parent.mkdir(parents=True, exist_ok=True)
            self._source_path.rename(self._tmp_storage_path)
        else:
            self._extract_archive()

        # Refuse to store files if Metax has conflicting files. See
        # https://jira.ci.csc.fi/browse/TPASPKT-749 for more
        # information.
        new_files = []
        for dirpath, _, files in os.walk(self._tmp_project_directory):
            for fname in files:
                new_files.append(
                    Path(dirpath, fname).relative_to(
                        self._tmp_project_directory
                    )
                )
        metax_client = get_metax_client()
        if len(new_files) == 1:
            # Creating metadata for only one file, so it is probably
            # more efficient to retrieve information about single file
            try:
                old_file = metax_client.get_project_file(
                    self.project.id,
                    str(self.path)
                )
                shutil.rmtree(self._tmp_path)
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
            old_files = metax_client.get_files_dict(self.project.id).keys()
            for file in new_files:
                if f"/{file}" in old_files:
                    conflicts.append(str(file))
            if conflicts:
                shutil.rmtree(self._tmp_path)
                raise UploadConflictError(
                    'Metadata could not be created because some files '
                    'already have metadata', files=conflicts
                )

        # Generate metadata
        metadata_dicts = []  # File metadata for Metax
        file_documents = []  # Basic file information to database
        for dirpath, _, files in os.walk(self._tmp_project_directory):
            for fname in files:
                file = Path(dirpath, fname)
                relative_path = file.relative_to(self._tmp_project_directory)

                # Create file information for database
                identifier = str(uuid.uuid4().urn)
                file_checksum_provided = (
                    self.type_ == UploadType.FILE
                    and self.source_checksum
                )
                if file_checksum_provided:
                    checksum = self.source_checksum
                else:
                    checksum = get_file_checksum("md5", file)

                file_documents.append(
                    FileEntry(
                        path=str(self.project.directory / relative_path),
                        checksum=checksum,
                        identifier=identifier
                    )
                )

                # Create metadata
                timestamp = _iso8601_timestamp(file)
                metadata_dicts.append({
                    "identifier": identifier,
                    "file_name": file.name,
                    "file_format": _get_mimetype(file),
                    "byte_size": file.stat().st_size,
                    "file_path": f"/{relative_path}",
                    "project_identifier": self.project.id,
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
        _post_metadata(metadata_dicts)

        # Insert information of all files to database in one go
        FileEntry.objects.insert(file_documents)

        # Move files to project directory
        self._move_files_to_project_directory()

        # Remove temporary directory. The directory might contain
        # empty directories, it must be removed recursively.
        shutil.rmtree(self._tmp_path)

        # Update quota. Delete the upload first so that this upload
        # is not counted in the quota twice.
        self._db_upload.delete()
        self.project.update_used_quota()

        # Release file storage lock
        lock_manager = ProjectLockManager()
        lock_manager.release(self.project.id, self.storage_path)

    def _move_files_to_project_directory(self):
        """Move files to project directory."""
        for dirpath, _, files in os.walk(self._tmp_project_directory):
            for fname in files:
                _file = os.path.join(dirpath, fname)
                relative_path = Path(_file).relative_to(
                    self._tmp_project_directory
                )
                source_path = self._tmp_project_directory / relative_path
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
    """Return last access time in ISO 8601 format.

    :param fpath: File path
    """
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


def _strip_metax_response(metax_response):
    """Collect only the necessary fields from the metax response."""
    response = {"success": [], "failed": []}

    if "failed" in metax_response:
        response["failed"] = metax_response["failed"]

    if "success" in metax_response and metax_response["success"]:
        for file_md in metax_response["success"]:
            identifier = file_md["object"]["identifier"]
            file_path = file_md["object"]["file_path"]
            parent_dir = file_md["object"]["parent_directory"]["identifier"]
            checksum = file_md["object"]["checksum"]["value"]

            metadata = {
                "object": {
                    "identifier": identifier,
                    "file_path": file_path,
                    "parent_directory": {"identifier": parent_dir},
                    "checksum": {"value": checksum}
                }
            }

            response["success"].append(metadata)

    return response


def _post_metadata(metadata_dicts):
    """Post multiple file metadata dictionaries to Metax.

    :param metadata_dicts: List of file metadata dictionaries
    :returns: Stripped HTTP response returned by Metax.
              Success list contains succesfully generated file
              metadata in format:
              [
                  {
                      "object": {
                          "identifier": identifier,
                          "file_path": file_path,
                          "checksum": {"value": checksum},
                          "parent_directory": {
                              "identifier": identifier
                          }
                      }
                  },
                  .
                  .
                  .
              ]
    """
    metax_client = get_metax_client()
    metadata = []
    responses = []

    # Post file metadata to Metax, 5000 files at time. Larger amount
    # would cause performance issues.
    i = 0
    for metadata_dict in metadata_dicts:
        metadata.append(metadata_dict)

        i += 1
        if i % 5000 == 0:
            response = metax_client.post_file(metadata)
            responses.append(_strip_metax_response(response))
            metadata = []

    # POST remaining metadata
    if metadata:
        response = metax_client.post_file(metadata)
        responses.append(_strip_metax_response(response))

    # Merge all responses into one response
    response = {"success": [], "failed": []}
    for metax_response in responses:
        if "success" in metax_response:
            response["success"].extend(metax_response["success"])
        if "failed" in metax_response:
            response["failed"].extend(metax_response["failed"])

    return response
