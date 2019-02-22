"""Module for accessing user database
"""
import os
import hashlib
import binascii
import random
from string import ascii_letters, digits
from bson.binary import Binary

import pymongo
from flask import current_app, request, has_request_context, safe_join
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


def _get_abs_path(metax_path):
    """Returns actual path on disk from metax_path"""
    return os.path.join(current_app.config.get("UPLOAD_PATH"), metax_path[1:])


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
    for dirpath, _, files in os.walk(fpath):
        for fname in files:
            _file = os.path.join(dirpath, fname)
            size += os.path.getsize(_file)

    return size


def update_used_quota():
    """Update used quota of the user"""
    username = request.authorization.username
    user = UsersDoc(username)
    project = user.get_project()
    path = safe_join(
        current_app.config.get("UPLOAD_PATH"),
        secure_filename(project)
    )
    size = get_dir_size(path)

    user.set_used_quota(size)


def get_mongo_client():
    """Returns a MongoClient instance"""
    host = "localhost"
    port = 27017

    if has_request_context():
        host = current_app.config.get("MONGO_HOST", host)
        port = current_app.config.get("MONGO_PORT", port)

    return pymongo.MongoClient(host, port)


def get_all_users():
    """Returns a list of all the users in upload.users collection"""
    users = get_mongo_client().upload.users
    return sorted(users.find().distinct("_id"))


class UserExistsError(Exception):
    """Exception for trying to create a user, which already exists"""
    pass


class UserNotFoundError(Exception):
    """Exception for querying a user, which does not exist"""
    pass


class UsersDoc(object):
    """Class for managing users in the database"""

    def __init__(self, username, quota=5*1024**3):
        """Initializing UsersDoc instances

        :param username: Used as primary key _id
        """
        self.users = get_mongo_client().upload.users
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

    def create(self, project, password=None):
        """Adds new user to the database. Salt is always
        generated randomly, but password can be set
        by providing to optional argument password.

        :param project: Project the user is associated with
        :param password: Password of the created user
        :returns: The password
        """
        # Raise exception if user already exists
        if self.exists():
            raise UserExistsError("User '%s' already exists" % self.username)

        if password is not None:
            passwd = password
        else:
            passwd = _get_random_string(PASSWD_LEN)

        salt = _get_random_string(SALT_LEN)
        digest = hash_passwd(passwd, salt)

        self.users.insert_one(
            {
                "_id" : self.username,
                "project" : project,
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
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        self.users.delete_one({"_id" : self.username})

    def get(self):
        """Returns existing user
        """
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        return self.users.find_one({"_id" : self.username})

    def get_utf8(self):
        """Returns existing user with digest in utf8 format
        """
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        user = self.users.find_one({"_id": self.username})
        user["digest"] = binascii.hexlify(user["digest"])

        return user

    def get_quota(self):
        """Returns the overall quota of the user"""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        return self.users.find_one({"_id" : self.username})["quota"]

    def get_used_quota(self):
        """Returns the used quota of the user"""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        return self.users.find_one({"_id" : self.username})["used_quota"]

    def set_quota(self, quota):
        """Set the quota of the user"""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        self.users.update_one(
            {"_id" : self.username},
            {"$set" : {"quota" : quota}}
        )

    def set_used_quota(self, used_quota):
        """Set the used quota of the user"""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        self.users.update_one(
            {"_id" : self.username},
            {"$set" : {"used_quota" : used_quota}}
        )

    def get_project(self):
        """Get user project"""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        return self.users.find_one({"_id" : self.username})["project"]

    def exists(self):
        """Check if the user is found in the db"""
        return self.users.find_one({"_id" : self.username}) is not None


class FilesCol(object):
    """Class for managing files in the database"""

    def __init__(self):
        """Initializing FilesDoc instances"""
        self.files = get_mongo_client().upload.files

    def get_path(self, identifier):
        """Get file_path based on _id identifier"""
        return self.files.find_one({"_id": identifier})["file_path"]

    def insert(self, files):
        """Insert multiple files into the files collection.

        :param files: List of dicts {"_id": identifier, "file_path": file_path}
        :returns: Number of documents inserted
        """
        return len(self.files.insert_many(files).inserted_ids)

    def delete(self, ids):
        """Delete multiple documents from the files collection.

        :param ids: List of identifiers to be removed
        :returns: Number of documents deleted
        """
        return self.files.delete_many({"_id": {"$in": ids}}).deleted_count

    def insert_one(self, document):
        """Insert one file document.

        :param document: Dict {"_id": identifier, "file_path": file_path}
        :returns: None
        """
        self.files.insert_one(document)

    def delete_one(self, identifier):
        """Delete one file document.

        :param identifier: _id of the document to be removed
        :returns: Number of documents deleted
        """
        return self.files.delete_one({"_id": identifier}).deleted_count

    def get_all_ids(self):
        """Return a list of all identifiers stored"""
        documents = self.files.find()
        return [document["_id"] for document in documents]

    def store_identifiers(self, file_md_list):
        """Store file identifiers and paths on disk to Mongo.

        :param file_md_list: List of created file metadata returned by Metax
        :returns: None
        """
        documents = []

        for file_md in file_md_list:
            documents.append({
                "_id": file_md["object"]["identifier"],
                "file_path": _get_abs_path(file_md["object"]["file_path"])
            })

        self.insert(documents)


def init_db():
    """Initialize database by creating the admin user."""
    user = UsersDoc("admin")
    if user.exists():
        print "Database already initialized"
        return

    # Create admin user
    password = user.create("admin_project")

    # Read conf file
    with open("/etc/upload_rest_api.conf", "r") as conf_file:
        lines = conf_file.readlines()

    # Write new conf file
    with open("/etc/upload_rest_api.conf", "w") as conf_file:
        for line in lines:
            if not line.startswith('admin_password = "'):
                conf_file.write(line)

        conf_file.write('ADMIN_PASSWORD = "%s"' % password)

    print "Database initialized"


if __name__ == "__main__":
    init_db()
