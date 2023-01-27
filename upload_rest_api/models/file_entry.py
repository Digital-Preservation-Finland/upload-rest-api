"""FileEntry class."""
import pathlib
from mongoengine import Document, StringField, ValidationError

from upload_rest_api.config import CONFIG


def _validate_file_path(path_string):
    """Validate path.

    Raises error if path is not absolute path and subpath of some
    project directory.
    """
    path = pathlib.Path(path_string)

    if str(path.resolve()) != path_string:
        raise ValidationError("File path is not absolute")

    if pathlib.Path(CONFIG['UPLOAD_PROJECTS_PATH']) not in path.parent.parents:
        raise ValidationError(
            "File path is not subpath of any project directory"
        )


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
