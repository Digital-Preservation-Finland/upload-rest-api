"""UploadEntry Class."""
from datetime import datetime, timezone
from enum import Enum

from mongoengine import (BooleanField, DateTimeField, Document, EnumField,
                         LongField, ReferenceField, StringField,
                         ValidationError)


from upload_rest_api.models.project_entry import ProjectEntry
from upload_rest_api.security import InvalidPathError, parse_relative_user_path


class UploadType(Enum):
    """Upload type."""
    FILE = "file"
    ARCHIVE = "archive"


def _validate_upload_path(path):
    """Validate that the file path does not perform path escape
    """
    try:
        parse_relative_user_path(path.strip("/"))
    except InvalidPathError as exc:
        raise ValidationError("Path is invalid") from exc


class UploadEntry(Document):
    """Document of an active upload in the MongoDB database

    The underlying database document is created at the start of an
    upload and deleted once the upload is complete or fails.
    """
    # The identifier for this upload. Default value is an UUID, but
    # there is no set format for the identifier.
    id = StringField(primary_key=True, required=True)
    # Relative upload path for the file
    path = StringField(required=True, validation=_validate_upload_path)

    type_ = EnumField(UploadType, db_field="type")
    project = ReferenceField(ProjectEntry, required=True)
    source_checksum = StringField()

    is_tus_upload = BooleanField(default=False)

    started_at = DateTimeField(default=lambda: datetime.now(timezone.utc))

    # Size of the file to upload in bytes
    size = LongField(required=True)

    meta = {
        "collection": "uploads"
    }
