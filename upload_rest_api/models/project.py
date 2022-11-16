import os
from pathlib import Path

from mongoengine import Document, LongField, NotUniqueError, StringField

from upload_rest_api import models
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


class Project(Document):
    """Database entry for a project"""
    id = StringField(primary_key=True)

    used_quota = LongField(default=0)
    quota = LongField(default=0)

    meta = {"collection": "projects"}

    @classmethod
    def create(cls, identifier, quota=5 * 1024**3):
        """Create project and prepare the file storage directory.
        """
        project = cls(id=identifier, quota=int(quota))

        try:
            project.save(force_insert=True)
        except NotUniqueError as exc:
            raise ProjectExistsError(
                f"Project '{identifier}' already exists"
            ) from exc

        project.directory.mkdir(exist_ok=True)

        return project

    @property
    def directory(self):
        return self.get_project_directory(self.id)

    @property
    def remaining_quota(self):
        """Remaining quota as bytes"""
        return self.quota - self.used_quota

    def update_used_quota(self):
        """Update used quota of the project."""
        stored_size = _get_dir_size(self.directory)
        allocated_size = self._get_allocated_quota()
        self.used_quota = stored_size + allocated_size
        self.save()

    @classmethod
    def get_project_directory(cls, project_id):
        """Get the file system path to the project."""
        return parse_user_path(CONFIG["UPLOAD_PROJECTS_PATH"], project_id)

    @classmethod
    def get_trash_root(cls, project_id, trash_id):
        """
        Get the file system path to a project specific temporary trash
        directory used for deletion.
        """
        return parse_user_path(
            Path(CONFIG["UPLOAD_TRASH_PATH"]), trash_id, project_id
        )

    @classmethod
    def get_trash_path(cls, project_id, trash_id, file_path):
        """
        Get the file system path to a temporary trash directory
        for a project file/directory used for deletion.
        """
        return parse_user_path(
            cls.get_trash_root(project_id=project_id, trash_id=trash_id),
            file_path
        )

    @classmethod
    def get_upload_path(cls, project_id, file_path):
        """Get upload path for file.

        :param project_id: project identifier
        :param file_path: file path relative to project directory of user
        :returns: full path of file
        """
        if file_path == "*":
            # '*' is shorthand for the base directory.
            # This is used to maintain compatibility with Werkzeug's
            # 'secure_filename' function that would sanitize it into an empty
            # string.
            file_path = ""

        project_dir = cls.get_project_directory(project_id)
        upload_path = (project_dir / file_path).resolve()

        return parse_user_path(project_dir, upload_path)

    @classmethod
    def get_return_path(cls, project_id, fpath):
        """Get path relative to project directory.

        Splice project path from fpath and return the path shown to the user
        and POSTed to Metax.

        :param project_id: project identifier
        :param fpath: full path
        :returns: string presentation of relative path
        """
        if fpath == "*":
            # '*' is shorthand for the base directory.
            # This is used to maintain compatibility with Werkzeug's
            # 'secure_filename' function that would sanitize it into an empty
            # string
            fpath = ""

        path = Path(fpath).relative_to(
            cls.get_project_directory(project_id)
        )

        path_string = f"/{path}" if path != Path('.') else '/'

        return path_string

    def _get_allocated_quota(self):
        return models.UploadEntry.objects.filter(project=self.id).sum("size")
