"""Project class."""
from upload_rest_api.database import Projects, Database


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
