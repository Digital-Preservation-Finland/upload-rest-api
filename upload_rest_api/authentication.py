"""Module for authenticating users."""
from hmac import compare_digest

from upload_rest_api.config import CONFIG
import upload_rest_api.database as db
from flask import abort, g, request
from werkzeug.local import LocalProxy


class CurrentUser:
    """
    Instance describing the currently authenticated user and its permissions.

    The instance exposes different methods to check various permissions for
    the current user
    """
    def __init__(self, username=None, projects=None, admin=False):
        """
        Create a new CurrentUser instance

        :param str username: Username for the current user.
                             If None, no user is authenticated.
        :param list projects: List of projects the user is allowed to access.
                              If None, all of the user's projects are
                              accessible.
        :param bool admin: Whether the current user is an admin.
                           Admin has every permission available.
        """
        self.username = username
        self.projects = projects

        # Note that an administrator can also be `dpres-admin-rest-api`
        # that is performing certain privileged actions on behalf of the
        # user that is logged into the web UI.
        #
        # This distinction is necessary because we want an user who is
        # logged in the web UI to be able to create, list and delete tokens.
        # User tokens, on the other hand, cannot be used to list or delete
        # tokens, and are only used for project related actions such as
        # uploading files.
        self.admin = admin

    def is_allowed_to_access_project(self, username, project):
        """
        Check if the user has permission to the given project
        """
        if self.admin:
            return True

        if self.username != username:
            return False

        if self.projects is None or project in self.projects:
            return True

        return False

    def is_allowed_to_create_tokens(self):
        """
        Check if the user can create tokens
        """
        # Only the admin can create tokens
        return self.admin

    def is_allowed_to_delete_tokens(self, username):
        """
        Check if the user can delete tokens
        """
        # Only the admin can delete tokens
        return self.admin

    def is_allowed_to_list_tokens(self, username):
        """
        Check if the user is allowed to list metadata for created tokens
        for the given user
        """
        # Only the admin can list tokens
        return self.admin


# pylint: disable=invalid-name
current_user = LocalProxy(lambda: g.current_user)


def _auth_user_by_token():
    """Authenticate user using a token provided through Authorization header.

    If successful, `g.current_user` will be initialized.
    """
    authorization = request.headers.get("Authorization", None)

    if not authorization or not authorization.startswith("Bearer "):
        return False

    token = authorization.split(" ")[1]

    # Check for pre-configured admin token
    admin_token = CONFIG.get("ADMIN_TOKEN", None)

    if token == admin_token:
        g.current_user = CurrentUser(
            username="admin",
            projects=None,
            admin=True
        )
        return True

    # Check if it's a token in the database
    database = db.Database()

    try:
        data = database.tokens.get_and_validate(token=token)
        g.current_user = CurrentUser(
            username=data["username"],
            projects=data["projects"],
            admin=data["admin"]
        )
        return True
    except db.TokenInvalidError:
        # Token does not exist or expired
        return False


def _auth_user_by_password():
    """Authenticate user using HTTP Basic Auth.

    If successful, `g.current_user` will be initialized.
    """
    auth = request.authorization

    if not auth:
        # HTTP Basic Auth not in use
        return False

    username = auth.username
    password = auth.password

    user = db.Database().user(username)

    try:
        user = user.get()
    except db.UserNotFoundError:
        # Calculate digest even if user does not exist to avoid
        # leaking information about which users exist
        return compare_digest(b"hash"*16, db.hash_passwd("passwd", "salt"))

    salt = user["salt"]
    digest = user["digest"]

    result = compare_digest(digest, db.hash_passwd(password, salt))

    if result:
        g.current_user = CurrentUser(
            username=user["_id"],
            # HTTP Basic Auth grants access to all projects
            projects=None,
            admin=False
        )

    return result


def authenticate():
    """Authenticate username and password.

    Returns 401 - Unauthorized access for wrong username or password
    """
    # Try authenticating using token first
    if _auth_user_by_token():
        return

    # Use user + password as fallback
    if _auth_user_by_password():
        return

    # Neither authentication method worked, abort the request
    abort(401)
