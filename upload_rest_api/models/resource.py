"""File and Directory models."""
from __future__ import annotations

import abc
import logging
import os
import pathlib
import shutil
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from metax_access import (
    DS_STATE_IN_DIGITAL_PRESERVATION,
    DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE,
)

from upload_rest_api.config import CONFIG
from upload_rest_api.lock import ProjectLockManager
from upload_rest_api.metax import get_metax_client
from upload_rest_api.models.project import Project

if TYPE_CHECKING:
    from metax_access.metax import MetaxFile


LANGUAGE_IDENTIFIERS = {
    "http://lexvo.org/id/iso639-3/eng": "en",
    "http://lexvo.org/id/iso639-3/fin": "fi",
    "http://lexvo.org/id/iso639-3/swe": "sv"
}

LOGGER = logging.getLogger(__name__)


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
        LANGUAGE_IDENTIFIERS[language["url"]]
        for language in dataset["language"]
        if LANGUAGE_IDENTIFIERS.get(language["url"])
    ]

    return {
        "title": dataset["title"],
        "languages": language_identifiers,
        "identifier": dataset["id"],
        "preservation_state": dataset["preservation"]["state"]
    }


def get_resource(project_id, path):
    """Get existing file or directory.

    :param project_id: The identifier of project that owns the resource.
    :param path: Path of the resource.
    """
    resource = File(project_id=project_id, path=path)
    if not resource.exists:
        resource = Directory(project_id=project_id, path=path)
    if not resource.exists:
        raise FileNotFoundError('Resource does not exist')

    return resource


class Resource(abc.ABC):
    """Resource class."""

    def __init__(
            self, path, *,
            project_id: str | None = None,
            project: Project | None = None,
    ):
        """Initialize resource.

        :param path: Path of the resource.
        :param project_id: The identifier of project that owns the
                               resource. Mutually exclusive with `project`.
        :param project: Project that owns the resource. Mutually exclusive
                        with `project_id`.
        """
        path = str(path)  # Allow pathlib.Path objects or strings

        # Raise InvalidPathError on attempted path escape
        try:
            # TODO: Could we use something besides pathlib.Path here?
            # `resolve()` involves actual I/O even if we only use it to process
            # any `..` parts in the path.
            relative_path = pathlib.Path(
                '/root-test', path.strip('/')
            ).resolve().relative_to('/root-test')
        except ValueError as error:
            raise InvalidPathError('Invalid path') from error

        self.path = pathlib.Path('/', relative_path)

        if sum([bool(project), bool(project_id)]) != 1:
            raise ValueError(
                "Either 'project' or 'project_id' must be provided, not both."
            )

        if project:
            self.project = project
        elif project_id:
            self.project = Project.get(id=project_id)
        self._datasets = None

    @property
    def storage_path(self):
        """Absolute path of resource."""
        return self.project.directory / self.path.relative_to('/')

    @property
    @abc.abstractmethod
    def exists(self):
        """Returns True if resource already exists."""

    @abc.abstractmethod
    def _get_file_group(self):
        """Get file group of resource."""

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

    def __init__(
        self,
        file_metadata: None | MetaxFile = None,
        *args,
        **kwargs
    ):
        """Initialize File.

        If 'file_metadata' is provided, set it to prevent unnecessary
        Metax request later on.
        """
        self._file_metadata_ = file_metadata

        super().__init__(*args, **kwargs)

    @property
    def exists(self):
        return self.storage_path.is_file()

    def _get_file_group(self):
        """File group that that contains only this file."""
        return FileGroup([self])

    @property
    def _file_metadata(self):
        """The metadata for this file.

        This property is lazy, meaning the metadata is not retrieved
        until this property is accessed for the first time.
        """
        if not self._file_metadata_:
            self._file_metadata_ = get_metax_client().get_project_file(
                project=str(self.project.id),
                path=str(self.path)
            )

        return self._file_metadata_

    @property
    def identifier(self):
        """Return identifier of file."""
        # TODO: Why "storage_identifier" and not "id"?
        return self._file_metadata["storage_identifier"]

    @property
    def checksum(self):
        """Return checksum of file."""
        return self._file_metadata["checksum"]

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

            LOGGER.info("Deleted file %s", str(self.storage_path))

            return {'deleted_files_count': 1}


class Directory(Resource):
    """Directory class."""

    @property
    def exists(self):
        return self.storage_path.is_dir()

    @classmethod
    def create(cls, project_id, path):
        """Create a new directory for a project.

        :param project_id: Project identifier
        :param path: Relative path for the project

        :returns: Directory instance
        """
        directory = cls(project_id=project_id, path=path)
        lock_manager = ProjectLockManager()
        with lock_manager.lock(directory.project.id, directory.storage_path):
            directory.storage_path.mkdir(parents=True)
            LOGGER.info("Created directory %s", str(directory.storage_path))
        return directory

    def _get_entries(self):
        return list(os.scandir(self.storage_path))

    def get_files(self):
        """List of files in directory."""
        metax = get_metax_client()
        # Note that this dict will contain metadata of files that have
        # already been removed from upload-rest-api (see TPASPKT-749).
        # However, that metadata is not used for anything, so it is not
        # a problem.
        filename2file_entry = {
            file_entry["filename"]: file_entry
            for file_entry in metax.get_directory(
                project=self.project.id,
                path=self.path,
            )["files"]
        }

        return [
            File(
                project=self.project,
                path=self.path / entry.name,
                file_metadata=filename2file_entry[entry.name]
            )
            for entry in self._get_entries() if entry.is_file()
        ]

    def get_directories(self):
        """List of directories in directory."""
        return [
            Directory(project=self.project, path=self.path / entry.name)
            for entry in self._get_entries() if entry.is_dir()
        ]

    def _get_file_group(self):
        """Group of all files in directory and its subdirectories."""
        metax = get_metax_client()
        # Note that this dict will contain metadata of files that have
        # already been removed from upload-rest-api (see TPASPKT-749).
        # However, that metadata is not used for anything, so it is not
        # a problem.
        path2file_entry = {
            file_entry["pathname"]: file_entry
            for file_entry in metax.get_directory_files_recursively(
                project=self.project.id,
                path=str(self.path),
            )
        }
        files = []
        for dirpath, _, filenames in os.walk(self.storage_path):
            for file in filenames:
                path = pathlib.Path("/") \
                    / pathlib.Path(dirpath, file).relative_to(
                        self.project.directory
                    )

                files.append(
                    File(
                        project=self.project,
                        path=path,
                        file_metadata=path2file_entry[str(path)]
                    )
                )

        return FileGroup(files)

    def get_all_files(self):
        """List all files in directory and its subdirectories."""
        return self._get_file_group().files

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

        LOGGER.info("Deleted directory %s", str(self.storage_path))

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

        # Update used_quota
        self.project.update_used_quota()

        return len(expired_files)


class FileGroup:
    """Class for managing group of files efficiently."""

    def __init__(self, files):
        """Initialize file group."""
        self.files = files
        self._file2dataset = None
        self._datasets = None

    def _retrieve_all_dataset_metadata(self):
        """Retrieve dataset metadata from Metax."""
        metax_client = get_metax_client()

        # Retrieve file -> dataset(s) associations
        file_storage_identifiers \
            = [file.identifier for file in self.files]
        self._file2dataset \
            = metax_client.get_file2dataset_dict(file_storage_identifiers)
        # Retrieve metadata of all datasets associated to files
        all_dataset_ids = set()
        for dataset_ids in self._file2dataset.values():
            all_dataset_ids |= set(dataset_ids)
        if not all_dataset_ids:
            self._datasets = {}
        else:
            self._datasets = {}
            datasets = metax_client.get_datasets_by_ids(
                list(all_dataset_ids)
            )
            for dataset in datasets:
                self._datasets[dataset['id']] \
                    = _dataset_to_result(dataset)

    def get_datasets(self):
        """List of all datasets that contain any file of the group."""
        if self._datasets is None:
            # Retrive datasets
            self._retrieve_all_dataset_metadata()
        return list(self._datasets.values())

    def has_pending_dataset(self):
        """Check if any file of the group has a pending dataset."""
        datasets = self.get_datasets()
        return any(
            dataset["preservation_state"]
            < DS_STATE_IN_DIGITAL_PRESERVATION
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
            < DS_STATE_IN_DIGITAL_PRESERVATION
            or dataset["preservation_state"]
            == DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE
            for dataset in datasets
        )

    def delete(self):
        """Delete files of the group.

        Deletes each file from filesystem and file metadata from Metax.

        The metadata of files that are part of a dataset is not removed
        (unless the dataset is in digital preservation, see TPASPKT-749
        for more information).
        """
        if any(self.file_has_pending_dataset(file) for file in self.files):
            raise HasPendingDatasetError

        storage_identifiers = []
        storage_paths = []  # All storage_paths in one list
        for file in self.files:
            # Remove the actual file
            file.storage_path.unlink()
            LOGGER.info("Deleted file %s", str(file.storage_path))
            # add identifiers storage_path to lists for bulk removal
            # from databases
            storage_identifiers.append(file.identifier)
            storage_paths.append(str(file.storage_path))

        # Remove metadata from Metax.
        # The metadata of preserved files should not be removed (see
        # TPASPKT-749). Deleting files that have pending
        # datasets is not possible, so at this point we know that if
        # a file in "directory_files" has a dataset, it is in
        # preservation. Therefore, we do not have to check the
        # preservation state of every dataset (which would be very
        # inefficient), as we can just remove metadata of all files that
        # are not inlcuded in any dataset.
        metax_client = get_metax_client()
        file2datasets = metax_client.get_file2dataset_dict(storage_identifiers)
        files_without_datasets = [
            {
                "storage_identifier": storage_identifier,
                "storage_service": "pas",
            }
            for storage_identifier in storage_identifiers
            if not file2datasets.get(storage_identifier)
        ]
        if files_without_datasets:
            metax_client.delete_files(files_without_datasets)
