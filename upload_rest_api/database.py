"""Module for accessing user database
"""
import hashlib
import binascii
import random

from string import ascii_letters, digits
from bson.binary import Binary

import pymongo
from flask import abort


# Password vars
PASSWD_LEN = 20
SALT_LEN = 20

# Hashing vars
ITERATIONS = 200000
HASH_ALG = "sha512"


def _get_random_string(chars):
    """Generate and return random string of given number of
    ascii letters or digits.

    :param chars: Lenght of the string to generate
    :returns: Generated random string
    """
    passwd = ""
    for _ in range(chars):
        passwd += random.SystemRandom().choice(ascii_letters + digits)

    return passwd


def hash_passwd(password, salt):
    """Salt and hash password using PBKDF2 with HMAC PRNG and SHA512 hashing
    algorithm.

    :returns: hexadecimal representation of the 512 bit digest
    """
    digest = hashlib.pbkdf2_hmac(HASH_ALG, password, salt, ITERATIONS)
    return Binary(digest)


class User(object):
    """Class for managing users in the database"""


    def __init__(self, user, quota=5*1024**3):
        """Initializing User instances"""
        self.users = pymongo.MongoClient().authentication.users
        self.user = user
        self.quota = quota


    def __repr__(self):
        """User instance representation"""
        user = self.users.find_one({"_id" : self.user})

        if user is None:
            return "User not found"

        salt = user["salt"]
        quota = user["quota"]
        digest = binascii.hexlify(user["digest"])

        return "_id : %s\nquota : %d\nsalt : %s\ndigest : %s" % (
            self.user, quota, salt, digest
        )

    def create(self):
        """Adds new user to the authentication database
        """
        # Abort if user already exists
        if self.exists():
            abort(405)

        salt = _get_random_string(SALT_LEN)
        passwd = "test"#_get_random_string(PASSWD_LEN)
        digest = hash_passwd(passwd, salt)

        self.users.insert_one(
            {
                "_id" : self.user,
                "digest" : digest,
                "salt" : salt,
                "quota" : self.quota
            }
        )


    def delete(self):
        """Deletes existing user
        """
        # Abort if user does not exist
        if not self.exists():
            abort(404)

        self.users.delete_one({"_id" : self.user})


    def get(self):
        """Returns existing user
        """
        # Abort if user does not exist
        if not self.exists():
            abort(404)

        return self.users.find_one({"_id" : self.user})


    def get_quota(self):
        """Returns the quota of the user"""
        # Abort if user does not exist
        if not self.exists():
            abort(404)

        return self.users.find_one({"_id" : self.user})["quota"]


    def set_quota(self, quota):
        """Set the quota of the user"""
        # Abort if user does not exist
        if not self.exists():
            abort(404)

        self.users.update_one(
            {"_id" : self.user},
            {"$set" : {"quota" : quota}}
        )


    def exists(self):
        """Check if the user is found in the db"""
        return self.users.find({"_id" : self.user}).count() != 0


if __name__ == "__main__":
    for i in range(10):
        User(str(i)).create()

    User("5").delete()
