"""Module for authenticating users
"""
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

    # Iterate until the end of the shorter string
    n = min(len(hash1), len(hash2))

    diff = len(hash1) ^ len(hash2)
    for i in range(n):
        diff |= ord(hash1[i]) ^ ord(hash2[i])

    return diff == 0


def auth_user(user, password):
    """Authenticate user"""
    users = db.get_users()
    user = users.find_one({"_id" : user})
    salt = user["salt"]
    digest = user["digest"]

    return _slow_equals(digest, db.hash_passwd(password, salt))


if __name__ == "__main__":
    for i in range(10):
        print auth_user(str(i), "test")

    print auth_user("0", "test1")
