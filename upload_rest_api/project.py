"""Project class."""
from upload_rest_api.database import Projects, Database
from upload_rest_api.config import CONFIG


class Project():
    """Project class."""

    def __init__(self, identifier):
        """Initialize Project."""
        self.identifier = identifier
        self.database = Database()
        self._project = self.database.projects.get(self.identifier)

    @property
    def directory(self):
        """Path to project directory."""
        return Projects.get_project_directory(self.identifier)

    @property
    def used_quota(self):
        """Get used quota."""
        return self._project['used_quota']

    def remaining_quota(self):
        """Compute remaining quota."""
        allocated_quota = self.database.uploads.get_project_allocated_quota(
            self.identifier
        )
        return (
            self._project["quota"]  # User's total quota
            - self._project["used_quota"]  # Finished and saved uploads
            - allocated_quota  # Disk space allocated for unfinished uploads
        )

    def update_used_quota(self):
        """Update used quota."""
        self.database.projects.update_used_quota(
            self.identifier, CONFIG["UPLOAD_PROJECTS_PATH"]
        )

    def set_used_quota(self, used_quota):
        """Set used quota."""
        self.database.projects.set_used_quota(self.identifier, used_quota)
