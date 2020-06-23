"""Module for authenticating users
"""
from __future__ import unicode_literals

from hmac import compare_digest

from flask import request, abort

import upload_rest_api.database as db


def _auth_user(username, password):
    """Authenticate user"""
    user = db.Database().user(username)

    try:
        user = user.get()
    except db.UserNotFoundError:
        # Calculate digest even if user does not exist to avoid
        # leaking information about which users exist
        return compare_digest(b"hash"*16, db.hash_passwd("passwd", "salt"))

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
