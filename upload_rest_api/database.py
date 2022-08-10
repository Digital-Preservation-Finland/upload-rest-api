"""Module for accessing user database."""
import binascii
import datetime
import hashlib
import json
import os
import random
import re
import secrets
import time
import uuid
from pathlib import Path
from string import ascii_letters, digits

import dateutil
import dateutil.parser
import pymongo
from bson.binary import Binary
from bson.codec_options import CodecOptions
from bson.objectid import ObjectId
from pymongo.errors import DuplicateKeyError
from pymongo.operations import UpdateOne
from redis import Redis
from rq.exceptions import NoSuchJobError
from rq.job import Job

import upload_rest_api.config
from upload_rest_api.config import CONFIG
from upload_rest_api.utils import parse_user_path

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


class ProjectExistsError(Exception):
    """Exception for trying to create a project which already exists."""


class ProjectNotFoundError(Exception):
    """Exception for querying a project which does not exist."""


class TaskNotFoundError(Exception):
    """Exception for querying a task, which does not exist."""


class TokenInvalidError(Exception):
    """Exception for using invalid token.

    Token is invalid because it does not exist or it expired.
    """


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

    def user(self, username):
        """Return user."""
        return User(self.client, username)

    @property
    def files(self):
        """Return files collection."""
        return Files(self.client)

    @property
    def projects(self):
        """Return projects collection."""
        return Projects(self.client)

    @property
    def tasks(self):
        """Return tasks collection."""
        return Tasks(self.client)

    @property
    def uploads(self):
        """Return uploads collection."""
        return Uploads(self.client)

    @property
    def tokens(self):
        """Return tokens collection."""
        return Tokens(self.client)


class User:
    """Class for managing users in the database."""

    def __init__(self, client, username):
        """Initialize User instances.

        :param username: Used as primary key _id
        """
        self.users = client.upload.users
        self.username = username
        self.projects = []

    def __repr__(self):
        """User instance representation."""
        user = self.users.find_one({"_id": self.username})

        if user is None:
            return "User not found"

        salt = user["salt"]
        digest = binascii.hexlify(user["digest"])

        return f"_id: {self.username}\nsalt: {salt}\ndigest: {digest}"

    def create(self, projects=None, password=None):
        """Add new user to the database.

        Salt is always generated randomly, but password can be set by
        providing to optional argument password.

        :param projects: Projects the user is associated with.
        :param password: Password of the created user
        :returns: The password
        """
        if projects is None:
            projects = []

        if password is not None:
            passwd = password
        else:
            passwd = get_random_string(PASSWD_LEN)

        salt = get_random_string(SALT_LEN)
        digest = hash_passwd(passwd, salt)

        try:
            self.users.insert_one(
                {
                    "_id": self.username,
                    "projects": projects,
                    "digest": digest,
                    "salt": salt,
                }
            )
        except DuplicateKeyError as exc:
            raise UserExistsError(
                f"User '{self.username}' already exists"
            ) from exc

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

    def grant_project(self, project):
        """Grant user access to the given project."""
        database = Database()
        if not database.projects.get(project):
            raise ProjectNotFoundError(f"Project '{project}' not found")

        result = self.users.update_one(
            {"_id": self.username},
            {"$addToSet": {"projects": project}}
        )

        if result.matched_count == 0:
            raise UserNotFoundError(f"User '{self.username}' not found")

    def revoke_project(self, project):
        """Revoke user access to the given project."""
        result = self.users.update_one(
            {"_id": self.username},
            {"$pull": {"projects": project}}
        )

        if result.matched_count == 0:
            raise UserNotFoundError(f"User '{self.username}' not found")

    def delete(self):
        """Delete existing user."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError(f"User '{self.username}' not found")

        self.users.delete_one({"_id": self.username})

    def get(self):
        """Return existing user."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError(f"User '{self.username}' not found")

        return self.users.find_one({"_id": self.username})

    def get_utf8(self):
        """Return existing user with digest in utf8 format."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError(f"User '{self.username}' not found")

        user = self.users.find_one({"_id": self.username})
        user["digest"] = binascii.hexlify(user["digest"]).decode("utf-8")

        return user

    def get_quota(self):
        """Return the overall quota of the user."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError(f"User '{self.username}' not found")

        return self.users.find_one({"_id": self.username})["quota"]

    def get_used_quota(self):
        """Return the used quota of the user."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError(f"User '{self.username}' not found")

        return self.users.find_one({"_id": self.username})["used_quota"]

    def set_quota(self, quota):
        """Set the quota of the user."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError(f"User '{self.username}' not found")

        self.users.update_one(
            {"_id": self.username},
            {"$set": {"quota": quota}}
        )

    def set_used_quota(self, used_quota):
        """Set the used quota of the user."""
        # Raise exception if user does not exist
        if not self.exists():
            raise UserNotFoundError(f"User '{self.username}' not found")

        self.users.update_one(
            {"_id": self.username},
            {"$set": {"used_quota": used_quota}}
        )

    def get_projects(self):
        """Get user projects."""
        result = self.users.find_one({"_id": self.username})
        if not result:
            raise UserNotFoundError(f"User '{self.username}' not found")

        return result["projects"]

    def exists(self):
        """Check if the user is found in the db."""
        return self.users.find_one({"_id": self.username}) is not None


class Files:
    """Class for managing files in the database."""

    def __init__(self, client):
        """Initialize Files instances."""
        self.files = client.upload.files

    def get(self, file_path):
        """Get file document based on file path."""
        return self.files.find_one({"_id": file_path})

    def get_by_identifier(self, identifier):
        """Get file document based on file identifier."""
        return self.files.find_one({"identifier": identifier})

    def get_path(self, identifier):
        """Get file path based on file identifier."""
        _file = self.files.find_one({"identifier": identifier})
        if _file is None:
            return None

        return _file["_id"]

    def get_identifier(self, fpath):
        """Get identifier based on file_path."""
        _file = self.files.find_one({"_id": str(fpath)})

        if _file is None or "identifier" not in _file.keys():
            return None

        return _file["identifier"]

    def get_checksum(self, filepath):
        """Get checksum of a single file."""
        _file = self.files.find_one({"_id": filepath})
        if _file is None:
            return None

        return _file["checksum"]

    def insert(self, files):
        """Insert multiple files into the files collection.

        :param files: List of dicts {"path": file path
                                     "checksum": checksum,
                                     "identifier": identifier}
        :returns: Number of documents inserted
        """
        documents = [
            {
                "_id": file["path"],
                "checksum": file["checksum"],
                "identifier": file["identifier"],
            } for file in files
        ]
        return len(self.files.insert_many(documents).inserted_ids)

    def delete(self, paths):
        """Delete multiple documents, identified by file path, from the files
        collection.

        :param paths: List of paths to be removed
        :returns: Number of documents deleted
        """
        # Split list of file path names to delete into chunks as a workaround
        # to MongoDB's 16 MB query size limitation.
        # 10,000 path names per chunk is enough provided path names are no
        # longer than 1,600 characters.
        file_path_chunks = [
            paths[i:i+10000] for i in range(0, len(paths), 10000)
        ]

        deleted_count = 0
        for chunk in file_path_chunks:
            deleted_count += self.files.delete_many(
                {"_id": {"$in": chunk}}
            ).deleted_count

        return deleted_count

    def delete_one(self, file_path):
        """Delete one file document.

        :param file_path: file_path of the document to be removed
        :returns: Number of documents deleted
        """
        return self.files.delete_one({"_id": file_path}).deleted_count

    def iter_files_in_dir(self, file_path):
        """Retrieve all files recursively under the given path.

        :returns: List of file documents
        """
        escaped_file_path = re.escape(str(file_path))
        files = self.files.find(
            {"_id": {"$regex": f"^{escaped_file_path}/"}}
        )
        for file_ in files:
            yield file_

    def get_all_ids(self):
        """Return a list of all stored identifiers."""
        documents = self.files.find(
            {"identifier": {"$exists": True}},
            {"_id": False, "identifier": True}
        )
        return [document["identifier"] for document in documents]

    def get_all_checksums(self):
        """Return a list of all checksums stored."""
        documents = self.files.find({}, {"_id": False, "checksum": True})
        return [document["checksum"] for document in documents]

    def get_all_files(self):
        """Return a list of all file documents."""
        return list(self.iter_all_files())

    def iter_all_files(self):
        """Return an iterator of all file documents."""
        return self.files.find()

    def get_path_checksum_dict(self):
        """Return files as a dict of filepath as key and checksum as value."""
        documents = self.files.find({}, {"_id": True, "checksum": True})
        return {doc["_id"]: doc["checksum"] for doc in documents}


class Tasks:
    """Class for managing tasks in the database."""

    def __init__(self, client):
        """Initialize Tasks instance."""
        self.tasks = client.upload.tasks
        self.task_messages = client.upload.task_messages

    def create(self, project_id):
        """Create one task document.

        :param str project_id: project name
        :returns: str: task id as string
        """
        return self.tasks.insert_one({"project": project_id,
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

    def find(self, project_id, status):
        """Find tasks.

        Return number of tasks for user having certain status for
        project.

        :param str project_id: project name
        :param str status: status of task
        :param bool include_message: whether to include task messages
                                     in the results
        :returns: Found tasks
        """
        tasks = list(self.tasks.find({
            "project": project_id, "status": status
        }))

        return tasks

    def _sync_task_status(self, task):
        """Syncronize task status.

        Check if the corresponding MongoDB task is in the failed RQ
        queue. If it is, update the MongoDB task entry correspondingly.

        This is used when the exception handler that updates the MongoDB
        entry was not executed. One case where this can happen is if the
        worker is killed by the out-of-memory killer.
        """
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
            raise TaskNotFoundError(f"Task '{task_id}' not found")

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
            raise TaskNotFoundError(f"Task '{task_id}' not found")

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
            raise TaskNotFoundError(f"Task '{task_id}' not found")

    def get_all_tasks(self):
        """Return all tasks."""
        # Messages are *not* included in the response to prevent
        # unnecessary memory usage
        return self.tasks.find()


class Uploads:
    """Class for managing pending uploads in the database."""

    def __init__(self, client):
        """Initialize Tasks instance."""
        self.uploads = client.upload.uploads

    def create(self, project_id, upload_path, resource):
        """Create one upload document.

        :param str project_id: Project identifier
        :param str upload_path: Path to the final location of the file or
                                extraction destination
        :param resource: tus resource corresponding to the upload
        :returns: ID of the created upload instance
        """
        return self.uploads.insert_one({
            # Resource ID contains an UUID, making it safe to use as an
            # unique identifier
            "_id": resource.identifier,
            "upload_path": upload_path,
            "project": project_id,
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

    def delete(self, ids):
        """Delete multiple documents from the uploads collection.

        :param ids: List of identifiers to be removed
        :returns: Number of documents deleted
        """
        return self.uploads.delete_many({"_id": {"$in": ids}}).deleted_count

    def get_project_allocated_quota(self, project_id):
        """Get the amount of bytes currently allocated for an user's tus
        uploads.

        This can be checked to prevent the user from initiating too many
        uploads that would exhaust the user's quota.

        :param project_id: Project identifier
        """
        uploads = self.uploads.find({"project": project_id})

        return sum(upload["size"] for upload in uploads)


class Tokens:
    """Class for managing user tokens."""

    def __init__(self, client):
        """Initialize Tokens instance."""
        self.tokens = client.upload.get_collection(
            "tokens",
            CodecOptions(tz_aware=True, tzinfo=datetime.timezone.utc)
        )

    def create(
            self, name, username, projects, expiration_date=None,
            admin=False, session=False):
        """Create one token.

        :param str name: User-provided name for the token
        :param str username: Username the token is intended for
        :param list projects: List of projects that the token will grant access
                              to
        :param expiration_date: Optional expiration date as datetime.datetime
                                instance
        :param bool admin: Whether the token has admin privileges.
                           This means the token can be used for creating,
                           listing and removing tokens, among other things.
        :param bool session: Whether the token is a temporary session token.
                             Session tokens are automatically cleaned up
                             periodically without user interaction.
        """
        # Token contains 256 bits of randomness per Python doc recommendation
        token = f"fddps-{secrets.token_urlsafe(32)}"

        # Expiration date is optional, but if provided, it must have
        # time zone information
        is_valid_expiration_date = (
            not expiration_date
            or (
                isinstance(expiration_date, datetime.datetime)
                and expiration_date.tzinfo
            )
        )

        if not is_valid_expiration_date:
            raise TypeError("expiration_date is not a valid datetime object")

        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        data = {
            # _id is only used to identify the token, but it doesn't
            # allow authorization. This is used when the user wants to specify
            # a created token (eg. when deleting an existing token).
            "_id": str(uuid.uuid4()),
            "name": name,
            "username": username,
            "projects": projects,
            "token_hash": token_hash,
            "expiration_date": expiration_date,
            "session": session,
            "admin": admin
        }

        self.tokens.insert_one(data)
        self._cache_token_data_to_redis(data)

        # Include the token in the initial creation request.
        # Only the SHA256 hash will be stored in the database.
        data["token"] = token
        del data["token_hash"]

        return data

    @classmethod
    def _cache_token_data_to_redis(cls, data):
        """
        Cache given token data to Redis.

        :param dict data: Dictionary to cache, as returned by pymongo
        """
        redis = get_redis_connection()

        redis_data = data.copy()
        token_hash = redis_data["token_hash"]

        del redis_data["token_hash"]

        expiration_date = redis_data["expiration_date"]
        if expiration_date:
            redis_data["expiration_date"] = expiration_date.isoformat()

        redis.set(
            f"fddps-token:{token_hash}", json.dumps(redis_data),
            ex=30 * 60  # Cache token for 30 minutes
        )

    def get_by_token(self, token):
        """Get the token from the database using the token itself.

        .. note::

            This does not validate the token. Use `get_and_validate` instead
            if that is required.
        """
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()

        redis = get_redis_connection()

        result = redis.get(f"fddps-token:{token_hash}")
        if result:
            result = json.loads(result)
            if result["expiration_date"]:
                result["expiration_date"] = dateutil.parser.parse(
                    result["expiration_date"]
                )
        else:
            # Token not in Redis cache, use MongoDB instead
            result = self.tokens.find_one({
                "token_hash": token_hash
            })

            if result:
                self._cache_token_data_to_redis(result)

        if not result:
            raise ValueError("Token not found")

        return result

    def get_and_validate(self, token):
        """Get the token from the database and validate it.

        :raises TokenInvalidError: Token is invalid
        """
        try:
            result = self.get_by_token(token=token)
        except ValueError as exc:
            raise TokenInvalidError("Token not found") from exc

        if not result["expiration_date"]:
            # No expiration date, meaning token is automatically valid
            return result

        now = datetime.datetime.now(datetime.timezone.utc)

        if result["expiration_date"] < now:
            raise TokenInvalidError("Token has expired")

        return result

    def delete(self, identifier):
        """Delete the given token.

        :returns: Number of deleted documents, either 1 or 0
        """
        redis = get_redis_connection()

        # We need to know the token hash in order to delete it from Redis as
        # well
        token_hash = self.tokens.find_one({"_id": identifier})["token_hash"]

        result = self.tokens.delete_one({"_id": identifier}).deleted_count
        redis.delete(f"fddps-token:{token_hash}")

        return result

    def find(self, username):
        """Find all user-created tokens belonging to an user."""
        return list(
            self.tokens.find({"username": username, "session": False})
        )

    def clean_session_tokens(self):
        """Remove expired session tokens."""
        now = datetime.datetime.now(datetime.timezone.utc)

        return self.tokens.delete_many(
            {
                "session": True,
                "expiration_date": {"$lte": now, "$ne": None}
            }
        ).deleted_count


class Projects:
    """Class for managing projects."""

    def __init__(self, client):
        """Initialize Projects instance."""
        self.projects = client.upload.get_collection(
            "projects",
            CodecOptions(tz_aware=True, tzinfo=datetime.timezone.utc)
        )

    def create(self, identifier, quota=5 * 1024**3):
        """Create one project.

        :param str identifier: Project identifier.
                               Also used as the displayed name.
        :param int quota: Total quota for the project

        :returns: Project entry as a dict
        """
        result = {
            "_id": identifier,
            "used_quota": 0,
            "quota": int(quota)
        }

        try:
            self.projects.insert_one(result)
            self.get_project_directory(identifier).mkdir(exist_ok=True)
        except DuplicateKeyError as exc:
            raise ProjectExistsError(
                f"Project '{identifier}' already exists"
            ) from exc

        return result

    def set_quota(self, identifier, quota):
        """Change the quota for a project.

        :param str name: Project name
        :param int quota: New quota for the project
        """
        result = self.projects.update_one(
            {"_id": identifier},
            {"$set": {"quota": int(quota)}}
        )

        if result.matched_count == 0:
            raise ProjectNotFoundError(f"Project '{identifier}' not found")

    def set_used_quota(self, identifier, used_quota):
        """Set the used quota of the project."""
        result = self.projects.update_one(
            {"_id": identifier},
            {"$set": {"used_quota": used_quota}}
        )

        if result.matched_count == 0:
            raise ProjectNotFoundError(f"Project '{identifier}' not found")

    def update_used_quota(self, identifier, root_upload_path):
        """Update used quota of the project."""
        path = parse_user_path(root_upload_path, identifier)
        size = get_dir_size(path)
        self.set_used_quota(identifier, size)

    def delete(self, identifier):
        """Delete single project.

        :param str name: Project name
        :returns: Whether the project was found and deleted
        """
        return self.projects.delete_one({"_id": identifier}).deleted_count

    def get(self, identifier):
        """Return project document based on the identifier.

        :param str task_id: task identifier string
        :returns: task document
        """
        return self.projects.find_one({"_id": identifier})

    def get_all_projects(self):
        """Return all project documents.

        :returns: list of projects
        """
        return list(self.projects.find())

    @classmethod
    def get_project_directory(cls, project_id):
        """Get the file system path to the project."""
        conf = upload_rest_api.config.CONFIG
        return parse_user_path(conf["UPLOAD_PROJECTS_PATH"], project_id)

    @classmethod
    def get_trash_root(cls, project_id, trash_id):
        """
        Get the file system path to a project specific temporary trash
        directory used for deletion.
        """
        conf = upload_rest_api.config.CONFIG
        return parse_user_path(
            Path(conf["UPLOAD_TRASH_PATH"]), trash_id, project_id
        )

    @classmethod
    def get_trash_path(cls, project_id, trash_id, file_path):
        """
        Get the file system path to a temporary trash directory
        for a project file/directory used for deletion.
        """
        return parse_user_path(
            cls.get_trash_root(project_id=project_id, trash_id=trash_id),
            file_path
        )

    @classmethod
    def get_upload_path(cls, project_id, file_path):
        """Get upload path for file.

        :param project_id: project identifier
        :param file_path: file path relative to project directory of user
        :returns: full path of file
        """
        if file_path == "*":
            # '*' is shorthand for the base directory.
            # This is used to maintain compatibility with Werkzeug's
            # 'secure_filename' function that would sanitize it into an empty
            # string.
            file_path = ""

        project_dir = cls.get_project_directory(project_id)
        upload_path = (project_dir / file_path).resolve()

        return parse_user_path(project_dir, upload_path)

    @classmethod
    def get_return_path(cls, project_id, fpath):
        """Get path relative to project directory.

        Splice project path from fpath and return the path shown to the user
        and POSTed to Metax.

        :param project_id: project identifier
        :param fpath: full path
        :returns: string presentation of relative path
        """
        if fpath == "*":
            # '*' is shorthand for the base directory.
            # This is used to maintain compatibility with Werkzeug's
            # 'secure_filename' function that would sanitize it into an empty
            # string
            fpath = ""

        path = Path(fpath).relative_to(
            cls.get_project_directory(project_id)
        )

        path_string = f"/{path}" if path != Path('.') else '/'

        return path_string


def get_redis_connection():
    """Get Redis connection."""
    password = CONFIG.get("REDIS_PASSWORD", None)
    redis = Redis(
        host=CONFIG["REDIS_HOST"],
        port=CONFIG["REDIS_PORT"],
        db=CONFIG["REDIS_DB"],
        password=password if password else None
    )

    return redis
