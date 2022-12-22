"""ProjectEntry class."""
from mongoengine import Document, LongField, StringField


class ProjectEntry(Document):
    """Database entry for a project"""
    id = StringField(primary_key=True)

    used_quota = LongField(default=0)
    quota = LongField(default=0)

    meta = {"collection": "projects"}

    @property
    def remaining_quota(self):
        """Remaining quota as bytes"""
        return self.quota - self.used_quota
