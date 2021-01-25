"""Module for accessing user database
"""
from __future__ import unicode_literals
from __future__ import print_function

import binascii
import hashlib
import os
import random
import time
from runpy import run_path
from string import ascii_letters, digits

import pymongo
from bson.binary import Binary
from bson.objectid import ObjectId

from flask import safe_join
from werkzeug.utils import secure_filename

# Password vars
PASSWD_LEN = 20
SALT_LEN = 20

# Hashing vars
ITERATIONS = 200000
HASH_ALG = "sha512"

# Default MongoDB document size is 4 MB, but let's use 2 MB just to be sure.
MESSAGE_CHUNK_SIZE = 2000000


def _iter_chunks(data, size=MESSAGE_CHUNK_SIZE):
    """
    Split data into chunks of given size and yield them one at a time
    """
    for i in range(0, len(data), size):
        yield data[i:i + size]


def get_random_string(chars):
    """Generate and return random string of given number of
    ascii letters or digits.

    :param chars: Lenght of the string to generate
    :returns: Generated random string
    """
    passwd = ""
    for _ in range(chars):
        passwd += random.SystemRandom().choice(ascii_letters + digits)

    return passwd


def _get_abs_path(metax_path, root_upload_path, project):
    """Returns actual path on disk from metax_path"""
    return os.path.join(
        root_upload_path,
        project,
        metax_path[1:]
    )


def hash_passwd(password, salt):
    """Salt and hash password using PBKDF2 with HMAC PRNG and SHA512 hashing
    algorithm.

    :returns: hexadecimal representation of the 512 bit digest
    """
    digest = hashlib.pbkdf2_hmac(
        HASH_ALG, password.encode("utf-8"), salt.encode("utf-8"), ITERATIONS)
    return Binary(digest)


def get_dir_size(fpath):
    """Returns the size of the dir fpath in bytes"""
    size = 0
    for dirpath, _, files in os.walk(fpath):
        for fname in files:
            _file = os.path.join(dirpath, fname)
            size += os.path.getsize(_file)

    return size


def parse_conf(fpath):
    """Parse config from file fpath"""
    return run_path(fpath)


class UserExistsError(Exception):
    """Exception for trying to create a user, which already exists"""
    pass


class UserNotFoundError(Exception):
    """Exception for querying a user, which does not exist"""
    pass


class TaskNotFoundError(Exception):
    """Exception for querying a task, which does not exist"""
    pass


class Database(object):
    """Class for accessing data in mongodb"""

    def __init__(self):
        """Initialize connection to mongodb"""
        conf = parse_conf("/etc/upload_rest_api.conf")
        self.client = pymongo.MongoClient(conf["MONGO_HOST"],
                                          conf["MONGO_PORT"])

    def get_all_users(self):
        """Returns a list of all the users in upload.users collection"""
        users = self.client.upload.users
        return sorted(users.find().distinct("_id"))

    def store_identifiers(self, file_md_list, root_upload_path, username):
        """Store file identifiers and paths on disk to Mongo.

        :param file_md_list: List of created file metadata returned by Metax
        :returns: None
        """
        documents = []
        project = self.user(username).get_project()

        for file_md in file_md_list:
            documents.append({
                "_id": file_md["object"]["identifier"],
                "file_path": _get_abs_path(file_md["object"]["file_path"],
                                           root_upload_path, project)
            })

        self.files.insert(documents)

    def user(self, username):
        """Returns user"""
        return User(self.client, username)

    @property
    def checksums(self):
        """Return checksums collection."""
        return Checksums(self.client)

    @property
    def files(self):
        """Return files collection."""
        return Files(self.client)

    @property
    def tasks(self):
        """Return tasks collection."""
        return Tasks(self.client)


class User(object):
    """Class for managing users in the database"""

    def __init__(self, client, username, quota=5*1024**3):
        """Initializing User instances

        :param username: Used as primary key _id
        """
        self.users = client.upload.users
        self.username = username
        self.quota = quota

    def __repr__(self):
        """User instance representation"""
        user = self.users.find_one({"_id": self.username})

        if user is None:
            return "User not found"

        salt = user["salt"]
        quota = user["quota"]
        digest = binascii.hexlify(user["digest"])

        return "_id : %s\nquota : %d\nsalt : %s\ndigest : %s" % (
            self.username, quota, salt, digest.decode("utf-8")
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
            passwd = get_random_string(PASSWD_LEN)

        salt = get_random_string(SALT_LEN)
        digest = hash_passwd(passwd, salt)

        self.users.insert_one(
            {
                "_id": self.username,
                "project": project,
                "digest": digest,
                "salt": salt,
                "quota": self.quota,
                "used_quota": 0
            }
        )

        return passwd

    def change_password(self):
        """Change user password
        """
        passwd = get_random_string(PASSWD_LEN)
        salt = get_random_string(SALT_LEN)
        digest = hash_passwd(passwd, salt)

        self.users.update_one(
            {"_id": self.username},
            {"$set": {
                "salt": salt,
                "digest": digest
            }}
        )

        return passwd

    def delete(self):
        """Deletes existing user
        """
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        self.users.delete_one({"_id": self.username})

    def get(self):
        """Returns existing user
        """
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        return self.users.find_one({"_id": self.username})

    def get_utf8(self):
        """Returns existing user with digest in utf8 format
        """
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        user = self.users.find_one({"_id": self.username})
        user["digest"] = binascii.hexlify(user["digest"]).decode("utf-8")

        return user

    def get_quota(self):
        """Returns the overall quota of the user"""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        return self.users.find_one({"_id": self.username})["quota"]

    def get_used_quota(self):
        """Returns the used quota of the user"""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        return self.users.find_one({"_id": self.username})["used_quota"]

    def set_quota(self, quota):
        """Set the quota of the user"""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        self.users.update_one(
            {"_id": self.username},
            {"$set": {"quota": quota}}
        )

    def set_used_quota(self, used_quota):
        """Set the used quota of the user"""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        self.users.update_one(
            {"_id": self.username},
            {"$set": {"used_quota": used_quota}}
        )

    def update_used_quota(self, root_upload_path):
        """Update used quota of the user"""
        project = self.get_project()
        path = safe_join(root_upload_path, secure_filename(project))
        size = get_dir_size(path)
        self.set_used_quota(size)

    def get_project(self):
        """Get user project"""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        return self.users.find_one({"_id": self.username})["project"]

    def set_project(self, project):
        """Set user project"""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        self.users.update_one(
            {"_id": self.username},
            {"$set": {"project": project}}
        )

    def exists(self):
        """Check if the user is found in the db"""
        return self.users.find_one({"_id": self.username}) is not None


class Checksums(object):
    """Class for managing checksums in the database"""

    def __init__(self, client):
        """Initializing Checksums instances"""
        self.checksums = client.upload.checksums

    def insert_one(self, filepath, checksum):
        """Insert a single checksum doc"""
        self.checksums.insert_one({"_id": filepath, "checksum": checksum})

    def insert(self, checksums):
        """Insert multiple checksum docs"""
        self.checksums.insert_many(checksums)

    def delete_one(self, filepath):
        """Delete a single checksum doc"""
        self.checksums.delete_one({"_id": filepath})

    def delete(self, filepaths):
        """Delete multiple checksum docs"""
        return self.checksums.delete_many(
            {"_id": {"$in": filepaths}}
        ).deleted_count

    def delete_dir(self, dirpath):
        """Delete all file checksums found under dirpath"""
        filepaths = []
        for _dir, _, files in os.walk(dirpath):
            for _file in files:
                fpath = os.path.join(_dir, _file)
                fpath = os.path.abspath(fpath)
                filepaths.append(fpath)

        self.delete(filepaths)

    def get_checksum(self, filepath):
        """Get checksum of a single file"""
        checksum = self.checksums.find_one({"_id": filepath})
        if checksum is None:
            return None

        return checksum["checksum"]

    def get_checksums(self):
        """Get all checksums"""
        return {
            i["_id"]: i["checksum"] for i in self.checksums.find({})
        }


class Files(object):
    """Class for managing files in the database"""

    def __init__(self, client):
        """Initializing Files instances"""
        self.files = client.upload.files

    def get_path(self, identifier):
        """Get file_path based on _id identifier"""
        _file = self.files.find_one({"_id": identifier})
        if _file is None:
            return None

        return _file["file_path"]

    def get_identifier(self, fpath):
        """Get file_identifier based on file_path"""
        _file = self.files.find_one({"file_path": fpath})

        if _file is None:
            return "None"

        return _file["_id"]

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


class Tasks(object):
    """Class for managing tasks in the database"""

    def __init__(self, client):
        """Initializing Tasks instance"""
        self.tasks = client.upload.tasks
        self.task_messages = client.upload.task_messages

    def create(self, project):
        """Creates one task document.

        :param str project: project name
        :returns: str: task id as string
        """
        return self.tasks.insert_one({"project": project,
                                      "timestamp": time.time(),
                                      "status": 'pending'}).inserted_id

    def delete_one(self, identifier):
        """Delete one task document.

        :param identifier: _id of the document to be removed
        :returns: Number of documents deleted
        """
        self.task_messages.delete_many({"task_id": ObjectId(identifier)})

        return self.tasks.delete_one(
            {"_id": ObjectId(identifier)}
        ).deleted_count

    def delete(self, ids):
        """Delete multiple documents from the tasks collection.

        :param ids: List of identifiers to be removed
        :returns: Number of documents deleted
        """
        obj_ids = []
        for identifier in ids:
            obj_ids.append(ObjectId(identifier))

        self.task_messages.delete_many({"task_id": {"$in": obj_ids}})
        return self.tasks.delete_many({"_id": {"$in": obj_ids}}).deleted_count

    def find(self, project, status):
        """Returns number of tasks for user having certain status for project.

        :param str project: project name
        :param str status: status of task
        :param bool include_message: whether to include task messages
                                     in the results
        :returns: Found tasks
        """
        tasks = list(self.tasks.find({"project": project, "status": status}))

        # Populate the message field by fetching the chunks, if any
        for task in tasks:
            message = self._get_message(task)
            if message:
                task["message"] = message

        return tasks

    def get(self, task_id):
        """Returns task document based on task_id.

        :param str task_id: task identifier string
        :returns: task document
        """
        task = self.tasks.find_one({"_id": ObjectId(task_id)})
        if task:
            message = self._get_message(task)
            if message:
                task["message"] = message

        return task

    def _get_message(self, task):
        """
        Get the individual chunk documents and return the reconstructed message
        """
        task_id = task["_id"]
        chunk_count = task.get("message_chunks", 0)

        if chunk_count == 0:
            return None

        chunks = self.task_messages.find({
            "task_id": task_id,
            "chunk_id": {"$in": list(range(0, chunk_count))}
        })
        chunks = list(chunks)
        chunks.sort(key=lambda entry: entry["chunk_id"])

        message = "".join([chunk["chunk"] for chunk in chunks])

        return message

    def _set_message(self, task_id, message):
        """
        Create the individual message chunks

        :returns: The amount of chunks used to save the message
        """
        chunk_count = 0
        for i, chunk in enumerate(_iter_chunks(message)):
            self.task_messages.update_one(
                {"task_id": task_id, "chunk_id": i},
                {
                    "$set": {
                        "task_id": task_id,
                        "chunk_id": i,
                        "chunk": chunk
                    }
                },
                {"upsert": True}
            )

            chunk_count += 1

        return chunk_count

    def update_status(self, task_id, status):
        """Updates status of the task.

        :param str task_id: task id as string
        :param str status: new status for the task
        """
        if not self.exists(task_id):
            raise TaskNotFoundError("Task '%s' not found" % task_id)
        self.tasks.update_one(
            {"_id": ObjectId(task_id)},
            {"$set": {"status": status}}
        )

    def update_message(self, task_id, message):
        """Updates message of the task.

        :param str task_id: task id as string
        :param str message: new message for the task
        """
        if not self.exists(task_id):
            raise TaskNotFoundError("Task '%s' not found" % task_id)

        chunk_count = self._set_message(
            task_id=ObjectId(task_id),
            message=message
        )
        self.tasks.update_one(
            {"_id": ObjectId(task_id)},
            {"$set": {"message_chunks": chunk_count}}
        )

    def update_md5(self, task_id, md5):
        """Updates md5 of the task.

        :param str task_id: task id as string
        :param str md5: new md5 for the task
        """
        if not self.exists(task_id):
            raise TaskNotFoundError("Task '%s' not found" % task_id)
        self.tasks.update_one(
            {"_id": ObjectId(task_id)},
            {"$set": {"md5": md5}}
        )

    def exists(self, task_id):
        """Check if the task is found in the db

        :param str task_id: task id as string
        :return True if task exists
        """
        return self.tasks.find_one({"_id": ObjectId(task_id)}) is not None

    def get_all_tasks(self):
        """Return all tasks"""
        # Messages are *not* included in the response to prevent unnecessary
        # memory usage
        return self.tasks.find()
