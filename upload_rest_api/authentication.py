"""Module for authenticating users
"""
from __future__ import unicode_literals

from hmac import compare_digest

from flask import request, abort

from upload_rest_api import database as db
from upload_rest_api.database import UserNotFoundError


def _auth_user(username, password, user=None):
    """Authenticate user"""
    if user is None:
        user = db.UsersDoc(username)

    try:
        user = user.get()
    except UserNotFoundError:
        # Calculate digest even if user does not exist to avoid
        # leaking information about which users exist
        return compare_digest("hash"*16, db.hash_passwd("passwd", "salt"))

    salt = user["salt"]
    digest = user["digest"]

    return compare_digest(digest, db.hash_passwd(password, salt))


def authenticate():
    """Authenticates username and password.

    Returns 401 - Unauthorized access for wrong username or password
    """
    auth = request.authorization
    if not auth or not _auth_user(auth.username, auth.password):
        abort(401)


def admin_only():
    """Checks that user trying to access resource is admin

    Returns 401 - Unauthorized access for wrong users
    """
    if request.authorization.username != "admin":
        abort(401)
