"""UploadEntry Class."""
from datetime import datetime, timezone
from enum import Enum

from mongoengine import (BooleanField, DateTimeField, Document, EnumField,
                         LongField, ReferenceField, StringField)


from upload_rest_api.models.project_entry import ProjectEntry


class UploadType(Enum):
    """Upload type."""
    FILE = "file"
    ARCHIVE = "archive"


class UploadEntry(Document):
    """Document of an active upload in the MongoDB database

    The underlying database document is created at the start of an
    upload and deleted once the upload is complete or fails.
    """
    # The identifier for this upload. Default value is an UUID, but
    # there is no set format for the identifier.
    id = StringField(primary_key=True, required=True)
    # Relative upload path for the file
    path = StringField(required=True)

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
