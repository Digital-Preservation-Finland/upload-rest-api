"""UserEntry class."""

from mongoengine import (BinaryField, Document, ListField,
                         StringField, ValidationError)

from upload_rest_api.models.project import ProjectEntry


def _validate_projects(projects):
    if len(projects) == 0:
        # Nothing to check
        return

    projects = set(projects)
    existing_projects = set(
        project.id for project in
        ProjectEntry.objects.filter(id__in=projects).only("id")
    )

    missing_projects = projects - existing_projects

    if missing_projects:
        raise ValidationError(
            f"Projects don't exist: {','.join(missing_projects)}"
        )


class UserExistsError(Exception):
    """Exception for trying to create a user, which already exists."""


class UserEntry(Document):
    """User database model"""
    username = StringField(primary_key=True, required=True)

    # Salt and digest if password authentication is enabled for this user.
    salt = StringField(required=False, null=True, default=None)
    digest = BinaryField(required=False, null=True, default=None)

    # Projects this user has access to
    projects = ListField(StringField(), validation=_validate_projects)

    meta = {"collection": "users"}
