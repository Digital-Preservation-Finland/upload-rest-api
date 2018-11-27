from string import ascii_letters, digits
import hashlib
import binascii
import random
from bson.binary import Binary

import pymongo
from flask import abort


# Password vars
PASSWD_LEN = 20
SALT_LEN = 20

# Hashing vars
ITERATIONS = 200000
HASH_ALG = "sha512"

DEFAULT_QUOTA = 5 * 1024**3


def _get_random_string(n):
    """Generate and return random string of n ascii letters or digits

    :param n: Lenght of the string to generate
    :returns: Generated random string
    """
    passwd = ""
    for _ in range(n):
        passwd += random.SystemRandom().choice(ascii_letters + digits)

    return passwd


def hash_passwd(password, salt):
    """Salt and hash password using PBKDF2 with HMAC PRNG and SHA512 hashing
    algorithm.

    :returns: hexadecimal representation of the 512 bit digest
    """
    digest = hashlib.pbkdf2_hmac(HASH_ALG, password, salt, ITERATIONS)
    return Binary(digest)


def get_users():
    """Opens pymongo client and returns users collection

    :returns: pymongo.MongoClient().Database.Collection
    """
    return pymongo.MongoClient().authentication.users


def create_user(user):
    """Adds new user to the authentication database
    """
    users = get_users()

    # Abort if user already exists
    if users.find({"_id": user}).count() > 0:
        abort(405)

    salt = _get_random_string(SALT_LEN)
    passwd = "test"#_get_random_string(PASSWD_LEN)
    digest = hash_passwd(passwd, salt)

    users.insert_one(
        {
            "_id" : user,
            "digest" : digest,
            "salt" : salt,
            "quota" : DEFAULT_QUOTA
        }
    )


if __name__ == "__main__":
    for i in range(10):
        create_user(str(i))
