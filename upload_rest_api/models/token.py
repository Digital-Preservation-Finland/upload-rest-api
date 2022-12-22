"""Token model."""
import hashlib
import secrets
import uuid

from upload_rest_api.models.token_entry import TokenEntry


class TokenInvalidError(Exception):
    """Exception for using invalid token.

    Token is invalid because it does not exist or it expired.
    """


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
