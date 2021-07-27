"""Module for accessing user database."""
import binascii
import hashlib
import os
import pathlib
import random
import time
from string import ascii_letters, digits

import pymongo
from bson.binary import Binary
from bson.objectid import ObjectId
from flask import safe_join
from rq.exceptions import NoSuchJobError
from rq.job import Job
from werkzeug.utils import secure_filename

import upload_rest_api.config

# Password vars
PASSWD_LEN = 20
SALT_LEN = 20

# Hashing vars
ITERATIONS = 200000
HASH_ALG = "sha512"


def get_random_string(chars):
    """Generate random string.

    String contains given number of ascii letters or digits.

    :param chars: Lenght of the string to generate
    :returns: Generated random string
    """
    passwd = ""
    for _ in range(chars):
        passwd += random.SystemRandom().choice(ascii_letters + digits)

    return passwd


def _get_abs_path(metax_path, root_upload_path, project):
    """Return actual path on disk from metax_path."""
    return os.path.join(
        root_upload_path,
        project,
        metax_path[1:]
    )


def hash_passwd(password, salt):
    """Salt and hash password.

    PBKDF2 with HMAC PRNG and SHA512 hashing
    algorithm is used.

    :returns: hexadecimal representation of the 512 bit digest
    """
    digest = hashlib.pbkdf2_hmac(
        HASH_ALG, password.encode("utf-8"), salt.encode("utf-8"), ITERATIONS)
    return Binary(digest)


def get_dir_size(fpath):
    """Return the size of the dir fpath in bytes."""
    size = 0
    for dirpath, _, files in os.walk(fpath):
        for fname in files:
            _file = os.path.join(dirpath, fname)
            size += os.path.getsize(_file)

    return size


class UserExistsError(Exception):
    """Exception for trying to create a user, which already exists."""


class UserNotFoundError(Exception):
    """Exception for querying a user, which does not exist."""


class TaskNotFoundError(Exception):
    """Exception for querying a task, which does not exist."""


class Database:
    """Class for accessing data in mongodb."""

    def __init__(self):
        """Initialize connection to mongodb."""
        conf = upload_rest_api.config.CONFIG
        self.client = pymongo.MongoClient(conf["MONGO_HOST"],
                                          conf["MONGO_PORT"])

    def get_all_users(self):
        """Return a list of all the users in upload.users collection."""
        users = self.client.upload.users
        return sorted(users.find().distinct("_id"))

    def store_identifiers(self, file_md_list, root_upload_path, username):
        """Store file identifiers and paths on disk to Mongo.

        :param file_md_list: List of created file metadata returned by
                             Metax
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
        """Return user."""
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

    @property
    def uploads(self):
        """Return uploads collection"""
        return Uploads(self.client)


class User:
    """Class for managing users in the database."""

    def __init__(self, client, username, quota=5*1024**3):
        """Initialize User instances.

        :param username: Used as primary key _id
        """
        self.users = client.upload.users
        self.username = username
        self.quota = quota

    def __repr__(self):
        """User instance representation."""
        user = self.users.find_one({"_id": self.username})

        if user is None:
            return "User not found"

        salt = user["salt"]
        quota = user["quota"]
        digest = binascii.hexlify(user["digest"])

        return "_id : %s\nquota : %d\nsalt : %s\ndigest : %s" % (
            self.username, quota, salt, digest.decode("utf-8")
        )

    @property
    def project_directory(self):
        """Directory for project files."""
        conf = upload_rest_api.config.CONFIG
        return pathlib.Path(conf["UPLOAD_PATH"],
                            secure_filename(self.get_project()))

    def create(self, project, password=None):
        """Add new user to the database.

        Salt is always generated randomly, but password can be set by
        providing to optional argument password.

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

        self.project_directory.mkdir(exist_ok=True)

        return passwd

    def change_password(self):
        """Change user password."""
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
        """Delete existing user."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        self.users.delete_one({"_id": self.username})

    def get(self):
        """Return existing user."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        return self.users.find_one({"_id": self.username})

    def get_utf8(self):
        """Return existing user with digest in utf8 format."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        user = self.users.find_one({"_id": self.username})
        user["digest"] = binascii.hexlify(user["digest"]).decode("utf-8")

        return user

    def get_quota(self):
        """Return the overall quota of the user."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        return self.users.find_one({"_id": self.username})["quota"]

    def get_used_quota(self):
        """Return the used quota of the user."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        return self.users.find_one({"_id": self.username})["used_quota"]

    def set_quota(self, quota):
        """Set the quota of the user."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        self.users.update_one(
            {"_id": self.username},
            {"$set": {"quota": quota}}
        )

    def set_used_quota(self, used_quota):
        """Set the used quota of the user."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        self.users.update_one(
            {"_id": self.username},
            {"$set": {"used_quota": used_quota}}
        )

    def update_used_quota(self, root_upload_path):
        """Update used quota of the user."""
        project = self.get_project()
        path = safe_join(root_upload_path, secure_filename(project))
        size = get_dir_size(path)
        self.set_used_quota(size)

    def get_project(self):
        """Get user project."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        return self.users.find_one({"_id": self.username})["project"]

    def set_project(self, project):
        """Set user project."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError("User '%s' not found" % self.username)

        self.users.update_one(
            {"_id": self.username},
            {"$set": {"project": project}}
        )

    def exists(self):
        """Check if the user is found in the db."""
        return self.users.find_one({"_id": self.username}) is not None


class Checksums:
    """Class for managing checksums in the database."""

    def __init__(self, client):
        """Initialize Checksums instances."""
        self.checksums = client.upload.checksums

    def insert_one(self, filepath, checksum):
        """Insert a single checksum doc."""
        self.checksums.insert_one({"_id": filepath, "checksum": checksum})

    def insert(self, checksums):
        """Insert multiple checksum docs."""
        self.checksums.insert_many(checksums)

    def delete_one(self, filepath):
        """Delete a single checksum doc."""
        self.checksums.delete_one({"_id": filepath})

    def delete(self, filepaths):
        """Delete multiple checksum docs."""
        return self.checksums.delete_many(
            {"_id": {"$in": filepaths}}
        ).deleted_count

    def delete_dir(self, dirpath):
        """Delete all file checksums found under dirpath."""
        filepaths = []
        for _dir, _, files in os.walk(dirpath):
            for _file in files:
                fpath = os.path.join(_dir, _file)
                fpath = os.path.abspath(fpath)
                filepaths.append(fpath)

        self.delete(filepaths)

    def get_checksum(self, filepath):
        """Get checksum of a single file."""
        checksum = self.checksums.find_one({"_id": filepath})
        if checksum is None:
            return None

        return checksum["checksum"]

    def get_checksums(self):
        """Get all checksums."""
        return {
            i["_id"]: i["checksum"] for i in self.checksums.find({})
        }


class Files:
    """Class for managing files in the database."""

    def __init__(self, client):
        """Initialize Files instances."""
        self.files = client.upload.files

    def get_path(self, identifier):
        """Get file_path based on _id identifier."""
        _file = self.files.find_one({"_id": identifier})
        if _file is None:
            return None

        return _file["file_path"]

    def get_identifier(self, fpath):
        """Get file_identifier based on file_path."""
        _file = self.files.find_one({"file_path": str(fpath)})

        if _file is None:
            return None

        return _file["_id"]

    def insert(self, files):
        """Insert multiple files into the files collection.

        :param files: List of dicts {"_id": identifier,
                                     "file_path": file_path}
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

        :param document: Dict {"_id": identifier,
                               "file_path": file_path}
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
        """Return a list of all identifiers stored."""
        documents = self.files.find()
        return [document["_id"] for document in documents]


class Tasks:
    """Class for managing tasks in the database."""

    def __init__(self, client):
        """Initialize Tasks instance."""
        self.tasks = client.upload.tasks
        self.task_messages = client.upload.task_messages

    def create(self, project):
        """Create one task document.

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
        """Return number of tasks for user having certain status for
        project.

        :param str project: project name
        :param str status: status of task
        :param bool include_message: whether to include task messages
                                     in the results
        :returns: Found tasks
        """
        tasks = list(self.tasks.find({"project": project, "status": status}))

        return tasks

    def _sync_task_status(self, task):
        """Check if the corresponding MongoDB task is in the failed RQ
        queue.

        If it is, update the MongoDB task entry correspondingly.

        This is used when the exception handler that updates the MongoDB
        entry was not executed. One case where this can happen is if the
        worker is killed by the out-of-memory killer.
        """
        from upload_rest_api.jobs.utils import get_redis_connection

        task_id = str(task["_id"])

        try:
            job = Job.fetch(task_id, connection=get_redis_connection())
        except NoSuchJobError:
            return task

        if job.is_failed and task["status"] != "error":
            self.update_status(task_id, "error")
            self.update_message(task_id, "Internal server error")
            task = self.tasks.find_one({"_id": ObjectId(task_id)})

        return task

    def get(self, task_id):
        """Return task document based on task_id.

        :param str task_id: task identifier string
        :returns: task document
        """
        task = self.tasks.find_one({"_id": ObjectId(task_id)})
        if task:
            # Synchronize RQ and MongoDB state if they're out of sync
            task = self._sync_task_status(task)

        return task

    def update_status(self, task_id, status):
        """Update status of the task.

        :param str task_id: task id as string
        :param str status: new status for the task
        """
        result = self.tasks.update_one(
            {"_id": ObjectId(task_id)},
            {"$set": {"status": status}}
        )

        if result.matched_count == 0:
            raise TaskNotFoundError("Task '%s' not found" % task_id)

    def update_message(self, task_id, message):
        """Update message of the task.

        :param str task_id: task id as string
        :param str message: new message for the task
        """
        result = self.tasks.update_one(
            {"_id": ObjectId(task_id)},
            {"$set": {"message": message}}
        )
        if result.matched_count == 0:
            raise TaskNotFoundError("Task '%s' not found" % task_id)

    def update_error(self, task_id, error_message, files=None):
        """Update error information of the task.

        :param str task_id: task id as string
        :param str error_message: Error message
        :param list files: Files that caused error
        """
        result = self.tasks.update_one(
            {
                "_id": ObjectId(task_id)
            },
            {
                "$set": {
                    "errors": [
                        {
                            "message": error_message,
                            "files": files
                        }
                    ]
                }
            }
        )

        if result.matched_count == 0:
            raise TaskNotFoundError("Task '%s' not found" % task_id)

    def get_all_tasks(self):
        """Return all tasks."""
        # Messages are *not* included in the response to prevent
        # unnecessary memory usage
        return self.tasks.find()


class Uploads:
    """Class for managing pending uploads in the database"""
    def __init__(self, client):
        """Initialize Tasks instance."""
        self.uploads = client.upload.uploads

    def create(self, user, file_path, resource):
        """Create one upload document.

        :param User user: User initiating the upload
        :param str file_path: Path to the final location of the file
        :param resource: tus resource corresponding to the upload
        :returns: ID of the created upload instance
        """
        return self.uploads.insert_one({
            # Resource ID contains an UUID, making it safe to use as an
            # unique identifier
            "_id": resource.identifier,
            "file_path": file_path,
            "username": user.username,
            "size": resource.upload_length
        }).inserted_id

    def delete_one(self, identifier):
        """Delete one Upload document.

        :param str identifier: Resource ID of the document to be removed
        :returns: Number of documents deleted

        .. note::

            This will *not* remove the corresponding tus workspace from disk.
        """
        return self.uploads.delete_one(
            {"_id": identifier}
        ).deleted_count

    def get_user_allocated_quota(self, user):
        """Get the amount of bytes currently allocated for an user's tus
        uploads.

        This can be checked to prevent the user from initiating too many
        uploads that would exhaust the user's quota.

        :param user: User initiating the upload
        """
        uploads = self.uploads.find({"username": user.username})

        return sum(upload["size"] for upload in uploads)
