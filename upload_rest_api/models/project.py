import os
import pathlib

from mongoengine import NotUniqueError

from upload_rest_api.models.project_entry import ProjectEntry
from upload_rest_api.models.upload_entry import UploadEntry
from upload_rest_api.config import CONFIG


def _get_dir_size(fpath):
    """Return the size of the dir fpath in bytes."""
    size = 0
    for dirpath, _, files in os.walk(fpath):
        for fname in files:
            _file = os.path.join(dirpath, fname)
            size += os.path.getsize(_file)

    return size


class ProjectExistsError(Exception):
    """Exception for trying to create a project which already exists."""


class Project:
    """
    Pre-ingest file storage project.

    Each project has its own directory on the file system, as well as an entry
    on the database to track quota.
    """
    def __init__(self, db_project):
        self._db_project = db_project

    # Read-only properties for database fields
    id = property(lambda x: x._db_project.id)
    quota = property(lambda x: x._db_project.quota)
    used_quota = property(lambda x: x._db_project.used_quota)

    remaining_quota = property(lambda x: x._db_project.remaining_quota)

    DoesNotExist = ProjectEntry.DoesNotExist

    @classmethod
    def create(cls, identifier, quota=5 * 1024**3):
        """Create project and prepare the file storage directory."""
        project = cls(
            db_project=ProjectEntry(id=identifier, quota=int(quota))
        )

        if project.directory.parents[0] \
                != pathlib.Path(CONFIG["UPLOAD_PROJECTS_PATH"]):
            raise ValueError('Invalid project identifier')

        try:
            project._db_project.save(force_insert=True)
        except NotUniqueError as exc:
            raise ProjectExistsError(
                f"Project '{identifier}' already exists"
            ) from exc

        project.directory.mkdir(exist_ok=True)

        return project

    @classmethod
    def get(cls, **kwargs):
        """
        Retrieve an existing project

        :param kwargs: Keyword arguments used to retrieve the project

        :returns: Project instance
        """
        return cls(
            db_project=ProjectEntry.objects.get(**kwargs)
        )

    @classmethod
    def list_all(cls):
        """List all existing projects.

        :returns: generator of all Project instances
        """
        return (cls(entry) for entry in ProjectEntry.objects)

    def delete(self):
        """
        Delete the project
        """
        self._db_project.delete()

    @property
    def directory(self):
        """Get the file system path to the project."""
        return pathlib.Path(CONFIG["UPLOAD_PROJECTS_PATH"], self.id)

    def set_quota(self, quota):
        """Set the quota for the project"""
        self._db_project.quota = quota
        self._db_project.save()

    def update_used_quota(self):
        """Update used quota of the project."""
        stored_size = _get_dir_size(self.directory)
        allocated_size \
            = UploadEntry.objects.filter(project=self.id).sum("size")
        self._db_project.used_quota = stored_size + allocated_size
        self._db_project.save()

    def increase_used_quota(self, quota):
        """Increase the used quota for this project.

        This is done when an archive extraction is started, and is necessary
        because we cannot know the final size of the extracted contents
        in advance.
        """
        self._db_project.used_quota += quota
        self._db_project.save()
