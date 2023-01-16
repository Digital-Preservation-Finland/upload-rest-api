"""File and Directory models."""

import os
import pathlib
import shutil
import time
from datetime import datetime, timezone

from metax_access import (DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
                          DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE,
                          DirectoryNotAvailableError)

from upload_rest_api.metax import metax_client
from upload_rest_api.config import CONFIG
from upload_rest_api import jobs
from upload_rest_api.lock import ProjectLockManager
from upload_rest_api.models.file_entry import FileEntry
from upload_rest_api.models.project import Project


LANGUAGE_IDENTIFIERS = {
    "http://lexvo.org/id/iso639-3/eng": "en",
    "http://lexvo.org/id/iso639-3/fin": "fi",
    "http://lexvo.org/id/iso639-3/swe": "sv"
}


class HasPendingDatasetError(Exception):
    """Pending dataset error.

    Raised if operation fails because resource is part of dataset that
    is being preserved.
    """


class InvalidPathError(Exception):
    """Invalid path error.

    Raised if path of the resource is invalid.
    """


def _dataset_to_result(dataset):
    """Extract basic information from dataset metadata.

    :param dataset: Raw dataset metadata from Metax
    """
    language_identifiers = [
        LANGUAGE_IDENTIFIERS[language["identifier"]]
        for language in dataset["research_dataset"].get("language", [])
        if LANGUAGE_IDENTIFIERS.get(language["identifier"], None)
    ]

    return {
        "title": dataset["research_dataset"]["title"],
        "languages": language_identifiers,
        "identifier": dataset["identifier"],
        "preservation_state": dataset["preservation_state"]
    }


def get_resource(project_id, path):
    """Get existing file or directory.

    :param project: The project that owns the resource.
    :param path: Path of the resource.
    """
    project = Project.get(id=project_id)

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

    @property
    def storage_path(self):
        """Absolute path of resource."""
        return self.project.directory / self.path.relative_to('/')

    def _get_file_group(self):
        """Empty file group."""
        # TODO: This dummy method could probably be removed
        return FileGroup([])

    def get_datasets(self):
        """List all datasets in which the resource has been added."""
        return self._get_file_group().get_datasets()

    def has_pending_dataset(self):
        """Check if resource has pending datasets.

        If the resource belongs to pending dataset, it can not be
        removed. If the resource belongs to preserved dataset it can be
        removed, but the metadata is not removed from Metax. See
        TPASPKT-749 for more information.
        """
        return self._get_file_group().has_pending_dataset()


class File(Resource):
    """File class."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._db_file_ = None

    @classmethod
    def get(cls, **kwargs):
        """
        Retrieve an existing file.

        :param kwargs: Keyword arguments used to retrieve the project

        :returns: File instance
        """
        try:
            entry = FileEntry.objects.get(**kwargs)
        except FileEntry.DoesNotExist as error:
            raise FileNotFoundError from error

        # Extract the project identifier from the file path stored in
        # database.
        # TODO: This stupid hack could be avoided if the project
        # identifier would be stored in the database
        project_id = pathlib.Path(entry.path)\
            .relative_to(CONFIG['UPLOAD_PROJECTS_PATH']).parts[0]

        project = Project.get(id=project_id)
        path = pathlib.Path(entry.path).relative_to(project.directory)
        return cls(project, path)

    def _get_file_group(self):
        """File group that that contains only this file."""
        return FileGroup([self])

    @property
    def _db_file(self):
        """
        The database entry for this file.

        This property is lazy, meaning the database entry is not retrieved
        until this property is accessed for the first time.
        """
        if not self._db_file_:
            self._db_file_ = FileEntry.objects.get(path=str(self.storage_path))

        return self._db_file_

    @property
    def identifier(self):
        """Return identifier of file."""
        return self._db_file.identifier

    @property
    def checksum(self):
        """Return checksum of file."""
        return self._db_file.checksum

    @property
    def timestamp(self):
        """Return last access time in ISO 8601 format."""
        timestamp = datetime.fromtimestamp(
            self.storage_path.stat().st_atime, tz=timezone.utc
        ).replace(microsecond=0)
        return timestamp.replace(microsecond=0).isoformat()

    @property
    def is_expired(self):
        """Check if the file is expired.

        Returns `True` if the file has not been accessed for time limit
        defined in configuration.
        """
        current_time = time.time()
        time_lim = CONFIG["CLEANUP_TIMELIM"]
        last_access = self.storage_path.stat().st_atime

        return current_time - last_access > time_lim

    def delete(self):
        """Delete file."""
        lock_manager = ProjectLockManager()
        with lock_manager.lock(self.project.id, self.storage_path):
            self._get_file_group().delete()

            self.project.update_used_quota()

            return {'deleted_files_count': 1}


class Directory(Resource):
    """Directory class."""

    @classmethod
    def create(cls, project_id, path):
        """
        Create a new directory for a project

        :param project_id: Project identifier
        :param path: Relative path for the project

        :returns: Directory instance
        """
        directory = cls(project=Project.get(id=project_id), path=path)
        lock_manager = ProjectLockManager()
        with lock_manager.lock(directory.project.id, directory.storage_path):
            directory.storage_path.mkdir(parents=True)
        return cls(project=project_id, path=path)

    @property
    def identifier(self):
        """Return identifier of directory."""
        try:
            return metax_client.get_project_directory(self.project.id,
                                                      self.path)['identifier']
        except DirectoryNotAvailableError:
            return None

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

    def _get_file_group(self):
        """Group of all files in directory and its subdirectories."""
        files = []
        for dirpath, _, filenames in os.walk(self.storage_path):
            for file in filenames:
                path = (pathlib.Path(dirpath) / file).relative_to(
                    self.project.directory
                )
                files.append(get_resource(self.project.id, path))

        return FileGroup(files)

    def get_all_files(self):
        """List all files in directory and its subdirectories."""
        return self._get_file_group().files

    def enqueue_delete_task(self):
        """Enqueue directory deletion task."""

        is_project_dir = self.storage_path.samefile(self.project.directory)
        if is_project_dir and not any(self.project.directory.iterdir()):
            raise FileNotFoundError('Project directory is empty')

        # Acquire a lock *and* keep it alive even after this HTTP
        # request. It will be released by the 'delete_directory'
        # background job once it finishes.
        lock_manager = ProjectLockManager()
        lock_manager.acquire(self.project.id, self.storage_path)

        try:
            task_id = jobs.enqueue_background_job(
                task_func="upload_rest_api.jobs.files.delete_directory",
                queue_name=jobs.FILES_QUEUE,
                project_id=self.project.id,
                job_kwargs={
                    "project_id": self.project.id,
                    "path": str(self.path),
                }
            )
        except Exception:
            # If we couldn't enqueue background job, release the lock
            lock_manager.release(self.project.id, self.storage_path)
            raise

        return task_id

    def delete(self):
        """Delete directory."""
        # Delete all files
        self._get_file_group().delete()

        # Remove directory from filesystem. Create new project directory
        # if the project directory was removed.
        shutil.rmtree(self.storage_path)
        self.project.directory.mkdir(exist_ok=True)

        # Update used_quota
        self.project.update_used_quota()

    def delete_expired_files(self):
        """Remove expired files.

        Remove all files in the directory and its subdirectories, that
        haven't been accessed within CLEANUP_TIMELIM. Files that are
        part of pending dataset are not removed.

        :returns: Number of deleted files
        """

        lock_manager = ProjectLockManager()
        with lock_manager.lock(self.project.id, self.storage_path):

            expired_files = []
            file_group = self._get_file_group()
            for file in file_group.files:
                if file.is_expired \
                        and not file_group.file_has_pending_dataset(file):
                    expired_files.append(file)

            if expired_files:
                FileGroup(expired_files).delete()

            return len(expired_files)

        # Update used_quota
        self.project.update_used_quota()


class FileGroup():
    """Class for managing group of files efficiently."""

    def __init__(self, files):
        """Initialize file group."""
        self.files = files
        self._file2dataset = None
        self._datasets = None

    def _retrieve_all_dataset_metadata(self):
        """Retrieve dataset metadata from Metax."""

        # Retrieve file -> dataset(s) associations
        file_identifiers \
            = [file.identifier for file in self.files]
        self._file2dataset \
            = metax_client.get_file2dataset_dict(file_identifiers)

        # Retrieve metadata of all datasets associated to files
        all_dataset_ids = set()
        for dataset_ids in self._file2dataset.values():
            all_dataset_ids |= set(dataset_ids)
        if not all_dataset_ids:
            self._datasets = {}
        else:
            # TODO: Maybe the Metax client should handle paging?
            count = 0
            offset = 0
            self._datasets = {}
            while True:
                result = metax_client.get_datasets_by_ids(
                    list(all_dataset_ids),
                    fields=["identifier", "preservation_state",
                            "research_dataset"],
                    offset=offset
                )

                for dataset in result["results"]:
                    self._datasets[dataset['identifier']] \
                        = _dataset_to_result(dataset)
                    count += 1

                if count >= result["count"]:
                    break

                offset += 1

    def get_datasets(self):
        """List of all files of the group."""
        if self._datasets is None:
            # Retrive datasets
            self._retrieve_all_dataset_metadata()

        return list(self._datasets.values())

    def has_pending_dataset(self):
        """Check if any file of the group has a pending dataset."""
        datasets = self.get_datasets()
        return any(
            dataset["preservation_state"]
            < DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
            or dataset["preservation_state"]
            == DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE
            for dataset in datasets
        )

    def get_file_datasets(self, file):
        """Get datasets of file."""
        if self._datasets is None:
            # Retrive datasets
            self._retrieve_all_dataset_metadata()

        datasets = []
        for dataset_id in self._file2dataset.get(file.identifier, []):
            datasets.append(self._datasets.get(dataset_id))

        return datasets

    def file_has_pending_dataset(self, file):
        """Check if file has pending dataset."""
        datasets = self.get_file_datasets(file)

        return any(
            dataset["preservation_state"]
            < DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
            or dataset["preservation_state"]
            == DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE
            for dataset in datasets
        )

    def delete(self):
        """Delete files of the group.

        Deletes each file from filesystem, database, and Metax.

        The metadata of files that are part of a dataset is not removed.

        :param files: List of files to be deleted
        """
        if any(self.file_has_pending_dataset(file) for file in self.files):
            raise HasPendingDatasetError

        identifiers = []
        storage_paths = []  # All storage_paths in one list
        for file in self.files:
            # Remove the actual file
            os.remove(file.storage_path)
            # add identifiers storage_path to lists for bulk removal
            # from databases
            identifiers.append(file.identifier)
            storage_paths.append(str(file.storage_path))

        # Remove all files from database
        FileEntry.objects.filter(path__in=storage_paths).delete()

        # Remove metadata from Metax.
        # The metadata of preserved files should not be removed (see
        # TPASPKT-749). Deleting files that have pending
        # datasets is not possible, so at this point we know that if
        # a file in "directory_files" has a dataset, it is in
        # preservation. Therefore, we do not have to check the
        # preservation state of every dataset (which would be very
        # inefficient), as we can just remove metadata of all files that
        # are not inlcuded in any dataset.
        file2datasets = metax_client.get_file2dataset_dict(identifiers)
        files_without_datasets = [
            identifier for identifier in identifiers if not file2datasets
        ]
        if files_without_datasets:
            metax_client.delete_files(files_without_datasets)
