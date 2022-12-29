"""FileEntry class."""
from mongoengine import Document, StringField, ValidationError

from upload_rest_api.security import parse_user_path, InvalidPathError


def _validate_file_path(path):
    """Validate that the file path is non-empty and starts with a slash."""
    if path == "":
        raise ValidationError("File path cannot be empty")
    if not path.startswith("/"):
        raise ValidationError("File path cannot be relative")

    try:
        parse_user_path(path)
    except InvalidPathError:
        raise ValidationError("Path is invalid")


class FileEntry(Document):
    """File stored in the pre-ingest file storage."""
    # Absolute file system path for the file. *NOT* the relative path
    # that is shown to the user.
    path = StringField(
        primary_key=True, required=True, validation=_validate_file_path
    )

    # MD5 checksum of the file
    checksum = StringField(required=True)
    # Metax identifier of the file
    identifier = StringField(required=True, unique=True)

    meta = {
        "collection": "files",
        # Do not auto create indexes. Otherwise, index will be created
        # on first query which can lead to slow performance until the creation
        # is finished, or a crash if the existing collection data conflicts
        # with the index parameters
        # (for example, unique index fails creation because there are
        #  already duplicate values in the collection).
        # Instead, create a CLI script that simply calls
        # `DocumentName.ensure_indexes()` that we can run during a maintenance
        # break.
        "auto_create_index": False,
        "indexes": [
            # Index created before MongoEngine migration
            {
                "name": "identifier_1",
                "fields": ["identifier"]
            }
        ]
    }

    @classmethod
    def get_path_checksum_dict(cls):
        """Return {filepath: checksum} dict of every file."""
        return {
            file_["_id"]: file_["checksum"]
            for file_ in cls.objects.only("path", "checksum").as_pymongo()
        }
