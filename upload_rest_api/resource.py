"""Resource class."""

import os
import pathlib
import secrets
import shutil

import metax_access
from metax_access import (DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
                          DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE)

from upload_rest_api import database
from upload_rest_api import gen_metadata
from upload_rest_api.config import CONFIG
from upload_rest_api.jobs.utils import FILES_QUEUE, enqueue_background_job
from upload_rest_api.lock import lock_manager
from upload_rest_api.upload import iso8601_timestamp


class HasPendingDatasetError(Exception):
    """Pending dataset error.

    Raised if operation fails because resource is part of dataset that
    is being preserved.
    """


def get_resource(project, path):
    """Get existing file or directory."""
    resource = Resource(project, path)
    if not resource.upload_path.exists():
        raise FileNotFoundError('Resource does not exist')
    if resource.upload_path.is_file():
        return File(project, path)
    if resource.upload_path.is_dir():
        return Directory(project, path)

    raise Exception('Resource is not file or directory')


class Resource():
    """Resource class."""

    def __init__(self, project, path):
        """Initialize resource."""
        self.path = pathlib.Path(path)
        self.project = project
        self.database = database.Database()
        self._datasets = None

    @property
    def upload_path(self):
        """Absolute path of resource."""
        return database.Projects.get_upload_path(self.project, self.path)

    @property
    def return_path(self):
        """Path of resource relative to project directory."""
        return database.Projects.get_return_path(self.project,
                                                 self.upload_path)

    def datasets(self):
        """List pending datasets."""
        metax = gen_metadata.MetaxClient()
        if self._datasets is None:
            self._datasets = metax.get_file_datasets(self.project,
                                                     self.upload_path)
        return self._datasets

    def has_pending_dataset(self):
        """Check if resource has pending datasets."""
        datasets = self.datasets()

        return any(
            dataset["preservation_state"]
            < DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
            or dataset["preservation_state"]
            == DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE
            for dataset in datasets
        )


class File(Resource):
    """File class."""

    def _document(self):
        return self.database.files.get(str(self.upload_path))

    @property
    def identifier(self):
        """Return identifier of file."""
        return self._document()["identifier"]

    @property
    def checksum(self):
        """Return checksum of file."""
        return self._document()["checksum"]

    @property
    def timestamp(self):
        """Creation date of file."""
        return iso8601_timestamp(self.upload_path)

    def delete(self):
        """Delete file."""
        if self.has_pending_dataset():
            raise HasPendingDatasetError

        root_upload_path = CONFIG.get("UPLOAD_PROJECTS_PATH")

        with lock_manager.lock(self.project, self.upload_path):
            # Remove metadata from Metax
            try:
                metax_client = gen_metadata.MetaxClient()
                metax_response = metax_client.delete_file_metadata(
                    self.project,
                    self.upload_path,
                    root_upload_path
                )
            except gen_metadata.MetaxClientError as exception:
                metax_response = str(exception)

            # Remove checksum and identifier from mongo
            self.database.files.delete_one(os.path.abspath(self.upload_path))
            os.remove(self.upload_path)

            self.database.projects.update_used_quota(self.project,
                                                     root_upload_path)

            return metax_response


class Directory(Resource):
    """Directory class."""

    @property
    def identifier(self):
        """Return identifier of directory."""
        metax = metax_access.Metax(
            url=CONFIG.get("METAX_URL"),
            user=CONFIG.get("METAX_USER"),
            password=CONFIG.get("METAX_PASSWORD"),
            verify=CONFIG.get("METAX_SSL_VERIFICATION")
        )
        return metax.get_project_directory(self.project,
                                           self.path)['identifier']

    def _entries(self):
        return list(os.scandir(self.upload_path))

    def files(self):
        """List of files in directory."""
        return [File(self.project, self.path / entry.name)
                for entry in self._entries() if entry.is_file()]

    def directories(self):
        """List of directories in directory."""
        return [Directory(self.project, self.path / entry.name)
                for entry in self._entries() if entry.is_dir()]

    def delete(self):
        """Delete directory."""
        if self.has_pending_dataset():
            raise HasPendingDatasetError

        project_dir = database.Projects.get_project_directory(self.project)
        is_project_dir = self.upload_path.samefile(project_dir)

        if is_project_dir and not any(project_dir.iterdir()):
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

        trash_root = self.database.projects.get_trash_root(
            project_id=self.project,
            trash_id=trash_id
        )
        trash_path = self.database.projects.get_trash_path(
            project_id=self.project,
            trash_id=trash_id,
            file_path=self.path
        )
        # Acquire a lock *and* keep it alive even after this HTTP
        # request. It will be released by the 'delete_files' background
        # job once it finishes.
        lock_manager.acquire(self.project, self.upload_path)

        try:
            try:
                trash_path.parent.mkdir(exist_ok=True, parents=True)
                self.upload_path.rename(trash_path)
            except FileNotFoundError as exception:
                # The directory to remove does not exist anymore;
                # other request managed to start deletion first.
                shutil.rmtree(trash_path.parent)
                raise FileNotFoundError("No files found") from exception

            if is_project_dir:
                # If we're deleting the entire project directory, create
                # an empty directory before proceeding with deletion
                project_dir.mkdir(exist_ok=True)

            # Remove all file metadata of files under fpath from Metax
            task_id = enqueue_background_job(
                task_func="upload_rest_api.jobs.files.delete_files",
                queue_name=FILES_QUEUE,
                project_id=self.project,
                job_kwargs={
                    "fpath": self.upload_path,
                    "trash_path": trash_path,
                    "trash_root": trash_root,
                    "project_id": self.project,
                }
            )
        except Exception:
            # If we couldn't enqueue background job, release the lock
            lock_manager.release(self.project, self.upload_path)
            raise

        return task_id
