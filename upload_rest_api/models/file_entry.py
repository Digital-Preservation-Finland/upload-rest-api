"""FileEntry class."""
from upload_rest_api.security import parse_user_path, InvalidPathError

from mongoengine import Document, QuerySet, StringField, ValidationError


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


class FileEntryQuerySet(QuerySet):
    """
    Custom query set for File documents that provides a function for
    deleting files in multiple queries to avoid hitting the MongoDB query
    size limit
    """
    def in_dir(self, path):
        """Filter query to only include files in given directory."""
        return self.filter(path__startswith=f"{path}/")

    def bulk_delete_by_paths(self, paths):
        """Delete multiple documents identified by file paths.

        The deletion is performed using multiple queries to avoid hitting
        the maximum query size limit.

        :param paths: List of paths to be removed
        """
        file_path_chunks = iter(
            paths[i:i+10000] for i in range(0, len(paths), 10000)
        )

        deleted_count = sum(
            self.filter(path__in=file_path_chunk).delete()
            for file_path_chunk in file_path_chunks
        )

        return deleted_count


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
        "queryset_class": FileEntryQuerySet,
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
