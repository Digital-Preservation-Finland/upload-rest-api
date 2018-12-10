"""Module for accessing user database
"""
import os
import hashlib
import binascii
import random
from string import ascii_letters, digits
from bson.binary import Binary

import pymongo
from flask import abort, current_app, has_request_context, safe_join
from werkzeug.utils import secure_filename


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


def get_dir_size(fpath):
    """Returns the size of the dir fpath in bytes"""
    size = 0
    for root, dirs, files in os.walk(fpath):
        for fname in files:
            _file = os.path.join(root, fname)
            size += os.path.getsize(_file)

    return size


def update_used_quota(request):
    """Update used quota of the user"""
    username = request.authorization.username
    user = User(username)
    path = safe_join(
        current_app.config.get("UPLOAD_PATH"),
        secure_filename(username)
    )
    size = get_dir_size(path)

    user.set_used_quota(size)


class User(object):
    """Class for managing users in the database"""


    def __init__(self, username, quota=5*1024**3):
        """Initializing User instances

        :param username:
        """
        host = "localhost"
        port = 27017

        if has_request_context():
            host = current_app.config.get("MONGO_HOST", host)
            port = current_app.config.get("MONGO_PORT", port)

        self.users = pymongo.MongoClient(host, port).auth.users
        self.username = username
        self.quota = quota


    def __repr__(self):
        """User instance representation"""
        user = self.users.find_one({"_id" : self.username})

        if user is None:
            return "User not found"

        salt = user["salt"]
        quota = user["quota"]
        digest = binascii.hexlify(user["digest"])

        return "_id : %s\nquota : %d\nsalt : %s\ndigest : %s" % (
            self.username, quota, salt, digest
        )


    def create(self, password=None):
        """Adds new user to the authentication database.
        Salt is always chosen randomly, but password can be set
        by providing to optional argument password.

        :param password: Password of the created user
        :returns: The password
        """
        # Abort if user already exists
        if self.exists():
            abort(405)

        if password is not None:
            passwd = password
        else:
            passwd = _get_random_string(PASSWD_LEN)

        salt = _get_random_string(SALT_LEN)
        digest = hash_passwd(passwd, salt)

        self.users.insert_one(
            {
                "_id" : self.username,
                "digest" : digest,
                "salt" : salt,
                "quota" : self.quota,
                "used_quota" : 0
            }
        )

        return passwd


    def delete(self):
        """Deletes existing user
        """
        # Abort if user does not exist
        if not self.exists():
            abort(404)

        self.users.delete_one({"_id" : self.username})


    def get(self):
        """Returns existing user
        """
        # Abort if user does not exist
        if not self.exists():
            abort(404)

        return self.users.find_one({"_id" : self.username})


    def get_utf8(self):
        """Returns existing user with digest in utf8 format
        """
        # Abort if user does not exist
        if not self.exists():
            abort(404)

        user = self.users.find_one({"_id": self.username})
        user["digest"] = binascii.hexlify(user["digest"])

        return user


    def get_quota(self):
        """Returns the overall quota of the user"""
        # Abort if user does not exist
        if not self.exists():
            abort(404)

        return self.users.find_one({"_id" : self.username})["quota"]


    def get_used_quota(self):
        """Returns the used quota of the user"""
        # Abort if user does not exist
        if not self.exists():
            abort(404)

        return self.users.find_one({"_id" : self.username})["used_quota"]


    def set_quota(self, quota):
        """Set the quota of the user"""
        # Abort if user does not exist
        if not self.exists():
            abort(404)

        self.users.update_one(
            {"_id" : self.username},
            {"$set" : {"quota" : quota}}
        )


    def set_used_quota(self, used_quota):
        """Set the used quota of the user"""
        # Abort if user does not exist
        if not self.exists():
            abort(404)

        self.users.update_one(
            {"_id" : self.username},
            {"$set" : {"used_quota" : used_quota}}
        )


    def exists(self):
        """Check if the user is found in the db"""
        return self.users.find_one({"_id" : self.username}) is not None
