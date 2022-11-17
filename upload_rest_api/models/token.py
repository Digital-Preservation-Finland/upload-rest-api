import datetime
import hashlib
import secrets
import uuid

from mongoengine import (BooleanField, DateTimeField, Document, ListField,
                         QuerySet, StringField, ValidationError)

from upload_rest_api.redis import get_redis_connection


class TokenInvalidError(Exception):
    """Exception for using invalid token.

    Token is invalid because it does not exist or it expired.
    """


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


class TokenEntryQuerySet(QuerySet):
    """
    Custom query set implementing bulk operations for tokens
    """
    def clean_session_tokens(self):
        """Remove expired session tokens."""
        now = datetime.datetime.now(datetime.timezone.utc)

        return self.filter(
            session=True, expiration_date__lte=now, expiration_date__ne=None
        ).delete()


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
    projects = ListField(StringField())

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
        "collection": "tokens",
        "queryset_class": TokenEntryQuerySet
    }

    @classmethod
    def create(
            cls, name, username, projects, expiration_date=None,
            admin=False, session=False):
        """Create one token.

        :param str name: User-provided name for the token
        :param str username: Username the token is intended for
        :param list projects: List of projects that the token will grant access
                              to
        :param expiration_date: Optional expiration date as datetime.datetime
                                instance
        :param bool admin: Whether the token has admin privileges.
                           This means the token can be used for creating,
                           listing and removing tokens, among other things.
        :param bool session: Whether the token is a temporary session token.
                             Session tokens are automatically cleaned up
                             periodically without user interaction.
        """
        # Token contains 256 bits of randomness per Python doc recommendation
        token = f"fddps-{secrets.token_urlsafe(32)}"

        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        new_token = cls(
            id=str(uuid.uuid4()),
            name=name,
            username=username,
            projects=projects,
            token_hash=token_hash,
            expiration_date=expiration_date,
            session=session,
            admin=admin
        )
        new_token.save()
        new_token._cache_token_to_redis()

        # Include the token in the initial creation request.
        # Only the SHA256 hash will be stored in the database.
        data = {
            "_id": new_token.id,
            "name": name,
            "username": username,
            "projects": projects,
            "expiration_date": expiration_date,
            "session": session,
            "admin": admin,
            "token": token
        }

        return data

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


class Token:
    """
    Authentication token for Pre-Ingest File Storage
    """
    def __init__(self, db_token):
        self._db_token = db_token

    # Read-only properties for database fields
    id = property(lambda x: x._db_token.id)
    name = property(lambda x: x._db_token.name)
    username = property(lambda x: x._db_token.username)
    projects = property(lambda x: tuple(x._db_token.projects))
    token_hash = property(lambda x: x._db_token.token_hash)
    expiration_date = property(lambda x: x._db_token.expiration_date)
    admin = property(lambda x: x._db_token.admin)
    session = property(lambda x: x._db_token.session)

    DoesNotExist = TokenEntry.DoesNotExist

    @classmethod
    def create(
            cls, name, username, projects, expiration_date=None,
            admin=False, session=False):
        """Create one token and return the token data as dict,
        including the token itself.

        Only the SHA256 hash will be stored in the database, meaning the
        plain-text token cannot be retrieved again later.

        :param str name: User-provided name for the token
        :param str username: Username the token is intended for
        :param list projects: List of projects that the token will grant access
                              to
        :param expiration_date: Optional expiration date as datetime.datetime
                                instance
        :param bool admin: Whether the token has admin privileges.
                           This means the token can be used for creating,
                           listing and removing tokens, among other things.
        :param bool session: Whether the token is a temporary session token.
                             Session tokens are automatically cleaned up
                             periodically without user interaction.

        :returns: Token fields as a dict, including the `token` field that
                  contains the plain-text token
        """
        # Token contains 256 bits of randomness per Python doc recommendation
        token = f"fddps-{secrets.token_urlsafe(32)}"

        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        new_token = TokenEntry(
            id=str(uuid.uuid4()),
            name=name,
            username=username,
            projects=projects,
            token_hash=token_hash,
            expiration_date=expiration_date,
            session=session,
            admin=admin
        )
        new_token.save()

        # Include the token in the initial creation request.
        # Only the SHA256 hash will be stored in the database.
        data = {
            "_id": new_token.id,
            "name": name,
            "username": username,
            "projects": projects,
            "expiration_date": expiration_date,
            "session": session,
            "admin": admin,
            "token": token
        }

        return data

    @classmethod
    def get(cls, **kwargs):
        """
        Retrieve an existing token

        :param kwargs: Field arguments used to retrieve the upload

        :returns: Token instance
        """
        return cls(
            db_token=TokenEntry.objects.get(**kwargs)
        )

    @classmethod
    def get_by_token(cls, token, validate=True):
        """
        Retrieve an existing token and optionally validate it.

        :param str token: Plain-text token
        :param bool validate: Whether to validate the token

        :raises TokenInvalidError: Token was validated and found to be invalid
        :raises Token.DoesNotExist: Token does not exist

        :returns: Token instance
        """
        token_ = TokenEntry.get_by_token(token)

        if validate and not token_.is_valid:
            raise TokenInvalidError("Token has expired")

        return token_

    def delete(self):
        """Delete the token."""
        self._db_token.delete()
