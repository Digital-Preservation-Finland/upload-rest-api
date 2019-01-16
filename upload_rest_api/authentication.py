"""Module for authenticating users
"""
from flask import request, abort

from upload_rest_api import database as db


def _slow_equals(hash1, hash2):
    """Function to compare hashes in O(n) time no matter
    how many common bytes they have. Bitwise XOR (^)
    is used for comparison to avoid branching and terminating
    immediately when difference is spotted. This function is used
    to negate timing attacks:

    https://crypto.stanford.edu/~dabo/papers/ssl-timing.pdf

    :param hash1: First hash to compare
    :param hash2: Second hash to compare
    :returns: True if identical else False
    """

    # Iterate until the end of the shorter hash
    hash_len = min(len(hash1), len(hash2))

    diff = len(hash1) ^ len(hash2)
    for i in range(hash_len):
        diff |= ord(hash1[i]) ^ ord(hash2[i])

    return diff == 0


def _auth_user(username, password, user=None):
    """Authenticate user"""
    if user is None:
        user = db.UsersDoc(username)

    if user.exists():
        user = user.get()
    else:
        # Calculate digest even if user does not exist to avoid
        # leaking information about which users exist
        return _slow_equals("hash"*16, db.hash_passwd("passwd", "salt"))

    salt = user["salt"]
    digest = user["digest"]

    return _slow_equals(digest, db.hash_passwd(password, salt))


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
