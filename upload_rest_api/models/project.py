import os
from pathlib import Path

from mongoengine import NotUniqueError

from upload_rest_api.models.project_entry import ProjectEntry
from upload_rest_api.models.upload_entry import UploadEntry
from upload_rest_api.config import CONFIG
from upload_rest_api.security import parse_user_path


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

    def delete(self):
        """
        Delete the project
        """
        self._db_project.delete()

    @property
    def directory(self):
        """Get the file system path to the project."""
        return parse_user_path(CONFIG["UPLOAD_PROJECTS_PATH"], self.id)

    def to_mongo(self):
        """
        Return the database entry as a dict with MongoDB data types
        """
        return self._db_project.to_mongo()

    def get_trash_root(self, trash_id):
        """
        Get the file system path to a project specific temporary trash
        directory used for deletion.

        :param str trash_id: Trash identifier

        :returns: Path instance of the trash root
        """
        return parse_user_path(
            Path(CONFIG["UPLOAD_TRASH_PATH"]), trash_id, self.id
        )

    def get_trash_path(self, trash_id, file_path):
        """
        Get the file system path to a temporary trash directory
        for a project file/directory used for deletion.

        :param str trash_id: Trash identifier
        :param file_path: Relative path used for deletion

        :rtype: pathlib.Path
        :returns: Trash path
        """
        return parse_user_path(
            self.get_trash_root(trash_id=trash_id), file_path
        )

    def get_upload_path(self, file_path):
        """Get upload path for file.

        :param file_path: file path relative to project directory of user
        :returns: full path of file
        """
        if file_path == "*":
            # '*' is shorthand for the base directory.
            # This is used to maintain compatibility with Werkzeug's
            # 'secure_filename' function that would sanitize it into an
            # empty string.
            file_path = ""

        upload_path = (self.directory / file_path).resolve()

        return parse_user_path(self.directory, upload_path)

    def get_return_path(self, fpath):
        """Get path relative to project directory.

        Splice project path from fpath and return the path shown to the user
        and POSTed to Metax.

        :param fpath: full path
        :returns: string presentation of relative path
        """
        if fpath == "*":
            # '*' is shorthand for the base directory.
            # This is used to maintain compatibility with Werkzeug's
            # 'secure_filename' function that would sanitize it into an
            # empty string
            fpath = ""

        path = Path(fpath).relative_to(self.directory)

        path_string = f"/{path}" if path != Path('.') else '/'

        return path_string

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
