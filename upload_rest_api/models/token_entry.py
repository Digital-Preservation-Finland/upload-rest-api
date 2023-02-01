"""TokenEntry class."""
import datetime
import hashlib
import uuid

from mongoengine import (BooleanField, DateTimeField, Document, ListField,
                         StringField, ValidationError)

from upload_rest_api.redis import get_redis_connection
from upload_rest_api.models.project_entry import ProjectEntry


def _validate_expiration_date(expiration_date):
    """
    Validate that the expiration date has time zone information if provided.
    """
    if isinstance(expiration_date, datetime.datetime) and \
            not expiration_date.tzinfo:
        raise ValidationError("Expiration date requires 'tzinfo'")


def _validate_uuid(value):
    try:
        uuid.UUID(value)
    except ValueError:
        raise ValidationError("Value is not a valid UUID")


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


class TokenEntry(Document):
    """Database entry for the Pre-Ingest File Storage authentication token"""
    # Identifier for the token.
    # UUIDField could be used here as it is more compact. However,
    # previous implementation used a normal string instead,
    # so only use field validation instead for backwards compatibility.
    id = StringField(primary_key=True, validation=_validate_uuid)

    # User-provided name for the token
    name = StringField()
    # User the token is intended for
    username = StringField(required=True)
    # List of projects this token grants access to
    projects = ListField(StringField(), validation=_validate_projects)

    # SHA256 token hash
    token_hash = StringField(required=True)
    expiration_date = DateTimeField(
        null=True, validation=_validate_expiration_date
    )

    # Whether the token has admin privileges.
    admin = BooleanField(default=False)

    # Whether the token is a temporary session token.
    # Session tokens are automatically cleaned up periodically without
    # user intereaction.
    session = BooleanField(default=False)

    meta = {
        "collection": "tokens"
    }

    @property
    def is_valid(self):
        """
        Token's validity. Valid tokens either have no expiration date, or the
        expiration date hasn't been exceeded yet.

        :returns: Whether the token is valid
        """
        if not self.expiration_date:
            # No expiration date, meaning token is automatically valid
            return True

        now = datetime.datetime.now(datetime.timezone.utc)

        return self.expiration_date > now

    def _cache_token_to_redis(self):
        """
        Cache given token data to Redis.

        :param dict data: Dictionary to cache, as returned by pymongo
        """
        redis = get_redis_connection()

        redis.set(
            f"fddps-token:{self.token_hash}", self.to_json(),
            ex=30 * 60  # Cache token for 30 minutes
        )

    @classmethod
    def get_by_token(cls, token):
        """Get the token from the database using the token itself."""
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()

        redis = get_redis_connection()

        result = redis.get(f"fddps-token:{token_hash}")
        if result:
            token_ = cls.from_json(result)
        else:
            # Token not in Redis cache, use MongoDB instead
            token_ = cls.objects.get(token_hash=token_hash)
            token_._cache_token_to_redis()

        return token_

    def delete(self):
        """Delete the given token.

        :returns: Number of deleted documents, either 1 or 0
        """
        redis = get_redis_connection()
        redis.delete(f"fddps-token:{self.token_hash}")

        return super().delete()
