"""Resource class."""

from datetime import datetime, timezone
import os
import pathlib
import secrets
import shutil

from metax_access import (DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
                          DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE)

from upload_rest_api import gen_metadata
from upload_rest_api.config import CONFIG
from upload_rest_api.jobs.utils import FILES_QUEUE, enqueue_background_job
from upload_rest_api.lock import lock_manager
from upload_rest_api.database import Project, DBFile


class HasPendingDatasetError(Exception):
    """Pending dataset error.

    Raised if operation fails because resource is part of dataset that
    is being preserved.
    """


class InvalidPathError(Exception):
    """Invalid path error.

    Raised if path of the resource is invalid.
    """


def get_resource(project_id, path):
    """Get existing file or directory.

    :param project: The project that owns the resource.
    :param path: Path of the resource.
    """
    project = Project.objects.get(id=project_id)

    resource = Resource(project, path)
    if resource.storage_path.is_file():
        return File(project, path)
    if resource.storage_path.is_dir():
        return Directory(project, path)
    if not resource.storage_path.exists():
        raise FileNotFoundError('Resource does not exist')

    raise Exception('Resource is not file or directory')


class Resource():
    """Resource class."""

    def __init__(self, project, path):
        """Initialize resource.

        :param Project project: The project that owns the resource.
        :param path: Path of the resource.
        """
        path = str(path)  # Allow pathlib.Path objects or strings

        # Raise InvalidPathError on attempted path escape
        try:
            relative_path = pathlib.Path(
                '/root', path.strip('/')
            ).resolve().relative_to('/root')
        except ValueError as error:
            raise InvalidPathError('Invalid path') from error

        self.path = pathlib.Path('/') / relative_path
        self.project = project
        self._datasets = None
        self.metax = gen_metadata.MetaxClient()

    @property
    def storage_path(self):
        """Absolute path of resource."""
        return self.project.directory / self.path.relative_to('/')

    def get_datasets(self):
        """List all datasets in which the resource has been added."""
        if self._datasets is None:
            self._datasets \
                = self.metax.get_file_datasets(self.project.id,
                                               self.storage_path)
        return self._datasets

    def has_pending_dataset(self):
        """Check if resource has pending datasets."""
        datasets = self.get_datasets()

        return any(
            dataset["preservation_state"]
            < DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
            or dataset["preservation_state"]
            == DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE
            for dataset in datasets
        )


class File(Resource):
    """File class."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._db_file = None

    @property
    def db_file(self):
        """
        The database entry for this file.

        This property is lazy, meaning the database entry is not retrieved
        until this property is accessed for the first time.
        """
        if not self._db_file:
            self._db_file = DBFile.objects.get(path=str(self.storage_path))

        return self._db_file

    @property
    def identifier(self):
        """Return identifier of file."""
        return self.db_file.identifier

    @property
    def checksum(self):
        """Return checksum of file."""
        return self.db_file.checksum

    @property
    def timestamp(self):
        """Return last access time in ISO 8601 format."""
        timestamp = datetime.fromtimestamp(
            self.storage_path.stat().st_atime, tz=timezone.utc
        ).replace(microsecond=0)
        return timestamp.replace(microsecond=0).isoformat()

    def delete(self):
        """Delete file."""
        if self.has_pending_dataset():
            raise HasPendingDatasetError

        root_upload_path = CONFIG.get("UPLOAD_PROJECTS_PATH")

        with lock_manager.lock(self.project.id, self.storage_path):
            # Remove metadata from Metax
            try:
                metax_response = self.metax.delete_file_metadata(
                    self.project.id,
                    self.storage_path,
                    root_upload_path
                )
            except gen_metadata.MetaxClientError as exception:
                metax_response = str(exception)

            # Remove checksum and identifier from mongo
            self.db_file.delete()
            os.remove(self.storage_path)

            self.project.update_used_quota()

            return metax_response


class Directory(Resource):
    """Directory class."""

    @property
    def identifier(self):
        """Return identifier of directory."""
        return self.metax.client.get_project_directory(self.project.id,
                                                       self.path)['identifier']

    def _get_entries(self):
        return list(os.scandir(self.storage_path))

    def get_files(self):
        """List of files in directory."""
        # TODO: Both 'get_files' and 'get_directories' are lazy when it comes
        # to database access. This means the underlying database entry
        # is not retrieved until it's accessed for the first time.
        # This can lead to a "N + 1 query" performance issue if the identifier
        # is retrieved from each file/directory, for example.
        # Instead, we should preload any database entries that already exist
        # in a bulk query and attach them to the instances here
        return [
            File(self.project, self.path / entry.name)
            for entry in self._get_entries() if entry.is_file()
        ]

    def get_directories(self):
        """List of directories in directory."""
        return [
            Directory(self.project, self.path / entry.name)
            for entry in self._get_entries() if entry.is_dir()
        ]

    def delete(self):
        """Delete directory."""
        if self.has_pending_dataset():
            raise HasPendingDatasetError

        is_project_dir = self.storage_path.samefile(self.project.directory)

        if is_project_dir and not any(self.project.directory.iterdir()):
            raise FileNotFoundError('Project directory is empty')

        # Create a random ID for the directory that will contain the
        # files and directories to delete. This is used to prevent
        # potential race conditions where the user creates and deletes a
        # directory/file while the previous directory/file is still
        # being deleted.
        # TODO: This pattern could be implemented in a more generic
        # manner and for other purposes besides deletion. In short:
        #
        # 1. Create temporary directory with unique ID with the same
        #    structure as the actual project directory
        # 2. Perform required operations (deletion, extraction) in the
        #    temporary directory
        # 3. Move the complete directory to the actual project directory
        #    atomically
        # 4. Delete the temporary directory
        trash_id = secrets.token_hex(8)

        trash_root = Project.get_trash_root(
            project_id=self.project.id,
            trash_id=trash_id
        )
        trash_path = Project.get_trash_path(
            project_id=self.project.id,
            trash_id=trash_id,
            file_path=self.path.relative_to('/')
        )
        # Acquire a lock *and* keep it alive even after this HTTP
        # request. It will be released by the 'delete_files' background
        # job once it finishes.
        lock_manager.acquire(self.project.id, self.storage_path)

        try:
            try:
                trash_path.parent.mkdir(exist_ok=True, parents=True)
                self.storage_path.rename(trash_path)
            except FileNotFoundError as exception:
                # The directory to remove does not exist anymore;
                # other request managed to start deletion first.
                shutil.rmtree(trash_path.parent)
                raise FileNotFoundError("No files found") from exception

            if is_project_dir:
                # If we're deleting the entire project directory, create
                # an empty directory before proceeding with deletion
                self.project.directory.mkdir(exist_ok=True)

            # Remove all file metadata of files under fpath from Metax
            task_id = enqueue_background_job(
                task_func="upload_rest_api.jobs.files.delete_files",
                queue_name=FILES_QUEUE,
                project_id=self.project.id,
                job_kwargs={
                    "fpath": self.storage_path,
                    "trash_path": trash_path,
                    "trash_root": trash_root,
                    "project_id": self.project.id,
                }
            )
        except Exception:
            # If we couldn't enqueue background job, release the lock
            lock_manager.release(self.project.id, self.storage_path)
            raise

        return task_id
