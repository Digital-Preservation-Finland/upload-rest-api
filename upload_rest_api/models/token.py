import datetime
import hashlib
import secrets
import uuid

from mongoengine import (BooleanField, DateTimeField, Document, ListField,
                         StringField, ValidationError)

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


class Token(Document):
    """Authentication token for the pre-ingest file storage."""
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

    meta = {"collection": "tokens"}

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
        """Get the token from the database using the token itself.

        .. note::

            This does not validate the token. Use `get_and_validate` instead
            if that is required.
        """
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()

        redis = get_redis_connection()

        result = redis.get(f"fddps-token:{token_hash}")
        if result:
            token_ = cls.from_json(result)
        else:
            # Token not in Redis cache, use MongoDB instead
            try:
                token_ = cls.objects.get(token_hash=token_hash)
                token_._cache_token_to_redis()
            except Token.DoesNotExist:
                return None

        return token_

    @classmethod
    def get_and_validate(cls, token):
        """Get the token from the database and validate it.

        :raises TokenInvalidError: Token is invalid
        """
        result = cls.get_by_token(token=token)

        if not result.expiration_date:
            # No expiration date, meaning token is automatically valid
            return result

        now = datetime.datetime.now(datetime.timezone.utc)

        if result.expiration_date < now:
            raise TokenInvalidError("Token has expired")

        return result

    def delete(self):
        """Delete the given token.

        :returns: Number of deleted documents, either 1 or 0
        """
        redis = get_redis_connection()
        redis.delete(f"fddps-token:{self.token_hash}")

        return super().delete()

    @classmethod
    def clean_session_tokens(cls):
        """Remove expired session tokens."""
        now = datetime.datetime.now(datetime.timezone.utc)

        return cls.objects.filter(
            session=True, expiration_date__lte=now, expiration_date__ne=None
        ).delete()
