"""FileEntry class."""
from mongoengine import Document, StringField


class FileEntry(Document):
    """File stored in the pre-ingest file storage."""
    path = StringField(primary_key=True, required=True)
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
