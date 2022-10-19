"""Module for accessing user database."""
import datetime
import hashlib
import logging
import os
import random
import secrets
import time
import uuid
from enum import Enum
from pathlib import Path
from string import ascii_letters, digits

from bson.binary import Binary
from mongoengine import (BinaryField, BooleanField, DateTimeField, DictField,
                         Document, EnumField, FloatField, ListField, LongField,
                         NotUniqueError, QuerySet, StringField, UUIDField,
                         ValidationError, connect)
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

try:
    connect(
        host=f"mongodb://{CONFIG['MONGO_HOST']}:{CONFIG['MONGO_PORT']}/upload",
        tz_aware=True
    )
except KeyError:
    logging.error(
        "MongoDB configuration missing, database connection not configured!"
    )


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


class ProjectExistsError(Exception):
    """Exception for trying to create a project which already exists."""


class TokenInvalidError(Exception):
    """Exception for using invalid token.

    Token is invalid because it does not exist or it expired.
    """


class User(Document):
    """Database collection for users"""
    username = StringField(primary_key=True, required=True)

    # Salt and digest if password authentication is enabled for this user.
    salt = StringField(required=False, null=True, default=None)
    digest = BinaryField(required=False, null=True, default=None)

    # Projects this user has access to
    projects = ListField(StringField())

    meta = {"collection": "users"}

    @classmethod
    def create(cls, username, projects=None, password=None):
        """Add new user to the database.

        Salt is always generated randomly, but password can be set by
        providing to optional argument password.

        :param projects: Projects the user is associated with.
        :param password: Password of the created user
        :returns: The password
        """
        new_user = cls(username=username)
        if projects is None:
            projects = []

        new_user.projects = projects

        if password is not None:
            passwd = password
        else:
            passwd = get_random_string(PASSWD_LEN)

        new_user.salt = get_random_string(SALT_LEN)
        new_user.digest = hash_passwd(passwd, new_user.salt)

        try:
            new_user.save(force_insert=True)
        except NotUniqueError as exc:
            raise UserExistsError(
                f"User '{username}' already exists"
            ) from exc
        return new_user

    def change_password(self):
        """Change user password."""
        passwd = get_random_string(PASSWD_LEN)
        self.salt = get_random_string(SALT_LEN)
        self.digest = hash_passwd(passwd, self.salt)
        self.save()

        return passwd

    def grant_project(self, project):
        """Grant user access to the given project."""
        project = Project.objects.get(id=project)

        if project not in self.projects:
            self.projects.append(project.id)

        self.save()

    def revoke_project(self, project):
        """Revoke user access to the given project."""
        self.projects.remove(project)
        self.save()


def _validate_file_path(path):
    """Validate that the file path is non-empty and starts with a slash."""
    if path == "":
        raise ValidationError("File path cannot be empty")
    if not path.startswith("/"):
        raise ValidationError("File path cannot be relative")


class FileQuerySet(QuerySet):
    """
    Custom query set for File documents that provides a function for
    deleting files in multiple queries to avoid hitting the MongoDB query
    size limit
    """
    def in_dir(self, path):
        """Filter query to only include files in given directory."""
        return self.filter(path__startswith=f"{path}/")

    def bulk_delete_by_paths(self, paths):
        """Delete multiple documents identified by file paths.

        The deletion is performed using multiple queries to avoid hitting
        the maximum query size limit.

        :param paths: List of paths to be removed
        """
        file_path_chunks = [
            paths[i:i+10000] for i in range(0, len(paths), 10000)
        ]

        deleted_count = sum(
            self.filter(path__in=file_path_chunk).delete()
            for file_path_chunk in file_path_chunks
        )

        return deleted_count


class DBFile(Document):
    """File stored in the pre-ingest file storage."""
    # Absolute file system path for the file. *NOT* the relative path
    # that is shown to the user.
    path = StringField(
        primary_key=True, required=True, validation=_validate_file_path
    )

    # MD5 checksum of the file
    checksum = StringField(required=True)
    # Metax identifier of the file
    identifier = StringField(required=True, unique=True)

    meta = {
        "collection": "files",
        "queryset_class": FileQuerySet,
        # Do not auto create indexes. Otherwise, index will be created
        # on first query which can lead to slow performance until the creation
        # is finished, or a crash if the existing collection data conflicts
        # with the index parameters
        # (for example, unique index fails creation because there are
        #  already duplicate values in the collection).
        # Instead, create a CLI script that simply calls
        # `DocumentName.ensure_indexes()` that we can run during a maintenance
        # break.
        "auto_create_index": False,
        "indexes": [
            # Index created before MongoEngine migration
            {
                "name": "identifier_1",
                "fields": ["identifier"]
            }
        ]
    }

    @classmethod
    def get_path_checksum_dict(cls):
        """Return {filepath: checksum} dict of every file."""
        return {
            file_["_id"]: file_["checksum"]
            for file_ in cls.objects.only("path", "checksum").as_pymongo()
        }


class TaskStatus(Enum):
    """Task status for background tasks."""
    PENDING = "pending"
    ERROR = "error"
    DONE = "done"


class TaskQuerySet(QuerySet):
    """
    Custom query set for Task documents that takes care of automatically
    synchronizing the state between the tasks on RQ and MongoDB.
    """
    def get(self, *args, **kwargs):
        """
        Custom getter that also checks the RQ at the same time and synchronizes
        the state for both if necessary
        """
        task = super().get(*args, **kwargs)

        task_id = str(task.id)

        try:
            job = Job.fetch(task_id, connection=get_redis_connection())
        except NoSuchJobError:
            return task

        # If the job has failed, update the status accordingly before
        # returning it to the user.
        if job.is_failed and task.status != TaskStatus.ERROR:
            task.status = TaskStatus.ERROR
            task.message = "Internal server error"
            task.save()

        return task


class Task(Document):
    """Background task."""
    project_id = StringField(required=True)
    # Task UNIX timestamp
    # TODO: Convert this to use the proper date type?
    timestamp = FloatField(null=False, default=time.time)

    # Status of the task
    status = EnumField(TaskStatus, default=TaskStatus.PENDING)
    # Optional status message for the task
    message = StringField(required=False)
    errors = ListField(DictField())

    meta = {
        "collection": "tasks",
        "queryset_class": TaskQuerySet
    }


class DBUpload(Document):
    """Database entry for an upload."""
    # The upload ID created by flask-tus-io, used to identify the upload.
    id = StringField(primary_key=True, required=True)
    # Absolute upload path for the file
    upload_path = StringField(required=True)
    project = StringField(required=True)

    # Size of the file to upload in bytes
    size = LongField(required=True)

    meta = {
        "collection": "uploads"
    }

    @classmethod
    def create(cls, project_id, upload_path, resource):
        """Create upload database entry from the given tus resource.
        """
        upload = cls(
            id=resource.identifier,
            project=project_id,
            upload_path=str(upload_path),
            size=resource.upload_length
        )
        upload.save(force_insert=True)

        return upload

    @classmethod
    def get_project_allocated_quota(cls, project_id):
        """Get the amount of bytes currently allocated for an user's tus
        uploads.

        This can be checked to prevent the user from initiating too many
        uploads that would exhaust the user's quota.

        :param project_id: Project identifier
        """
        return cls.objects.filter(project=project_id).sum("size")


def _validate_expiration_date(expiration_date):
    """
    Validate that the expiration date has time zone information if provided.
    """
    if isinstance(expiration_date, datetime.datetime) and \
            not expiration_date.tzinfo:
        raise ValidationError("Expiration date requires 'tzinfo'")


class Token(Document):
    """Authentication token for the pre-ingest file storage."""
    id = UUIDField(primary_key=True)

    # User-provided name for the token
    name = StringField()
    # User the token is intended for
    username = StringField(required=True)
    # List of projects this token grants access to
    projects = ListField(StringField())

    # SHA256 token hash
    token_hash = StringField(required=True)
    expiration_date = DateTimeField(
        null=True, validation=_validate_expiration_date
    )

    # Whether the token has admin privileges.
    admin = BooleanField(default=False)

    # Whether the token is a temporary session token.
    # Session tokens are automatically cleaned up periodically without
    # user intereaction.
    session = BooleanField(default=False)

    meta = {"collection": "tasks"}

    @classmethod
    def create(
            cls, name, username, projects, expiration_date=None,
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

        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        new_token = cls(
            id=str(uuid.uuid4()),
            name=name,
            username=username,
            projects=projects,
            token_hash=token_hash,
            expiration_date=expiration_date,
            session=session,
            admin=admin
        )
        new_token.save()
        new_token._cache_token_to_redis()

        # Include the token in the initial creation request.
        # Only the SHA256 hash will be stored in the database.
        data = {
            "_id": new_token.id,
            "name": name,
            "username": username,
            "projects": projects,
            "expiration_date": expiration_date,
            "session": session,
            "admin": admin,
            "token": token
        }

        return data

    def _cache_token_to_redis(self):
        """
        Cache given token data to Redis.

        :param dict data: Dictionary to cache, as returned by pymongo
        """
        redis = get_redis_connection()

        redis.set(
            f"fddps-token:{self.token_hash}", self.to_json(),
            ex=30 * 60  # Cache token for 30 minutes
        )

    @classmethod
    def get_by_token(cls, token):
        """Get the token from the database using the token itself.

        .. note::

            This does not validate the token. Use `get_and_validate` instead
            if that is required.
        """
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()

        redis = get_redis_connection()

        result = redis.get(f"fddps-token:{token_hash}")
        if result:
            token_ = cls.from_json(result)
        else:
            # Token not in Redis cache, use MongoDB instead
            token_ = cls.objects.get(token_hash=token_hash)

            if token_:
                token_._cache_token_to_redis()

        return token_

    @classmethod
    def get_and_validate(cls, token):
        """Get the token from the database and validate it.

        :raises TokenInvalidError: Token is invalid
        """
        result = cls.get_by_token(token=token)

        if not result.expiration_date:
            # No expiration date, meaning token is automatically valid
            return result

        now = datetime.datetime.now(datetime.timezone.utc)

        if result.expiration_date < now:
            raise TokenInvalidError("Token has expired")

        return result

    def delete(self):
        """Delete the given token.

        :returns: Number of deleted documents, either 1 or 0
        """
        redis = get_redis_connection()
        redis.delete(f"fddps-token:{self.token_hash}")

        return super().delete()

    @classmethod
    def find(cls, username):
        """Find all user-created tokens belonging to an user."""
        return cls.objects.filter(username=username, session=False)

    @classmethod
    def clean_session_tokens(cls):
        """Remove expired session tokens."""
        now = datetime.datetime.now(datetime.timezone.utc)

        return cls.objects.filter(
            session=True, expiration_date__lte=now, expiration_date__ne=None
        ).delete()


class Project(Document):
    """Database entry for a project"""
    id = StringField(primary_key=True)

    used_quota = LongField(default=0)
    quota = LongField(default=0)

    meta = {"collection": "projects"}

    @classmethod
    def create(cls, identifier, quota=5 * 1024**3):
        """Create project and prepare the file storage directory.
        """
        project = cls(id=identifier, quota=int(quota))

        try:
            project.save(force_insert=True)
        except NotUniqueError as exc:
            raise ProjectExistsError(
                f"Project '{identifier}' already exists"
            ) from exc

        project.directory.mkdir(exist_ok=True)

        return project

    @property
    def directory(self):
        return self.get_project_directory(self.id)

    @property
    def remaining_quota(self):
        """Remaining quota as bytes"""
        return self.quota - self.used_quota

    def update_used_quota(self):
        """Update used quota of the project."""
        stored_size = get_dir_size(self.directory)
        allocated_size = DBUpload.get_project_allocated_quota(self.id)
        self.used_quota = stored_size + allocated_size
        self.save()

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
