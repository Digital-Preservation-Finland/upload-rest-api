"""upload-rest-api models"""
# flake8: noqa
import logging

from mongoengine import connect

from upload_rest_api.config import CONFIG
from upload_rest_api.models.file import FileEntry
from upload_rest_api.models.project import (Project, ProjectEntry,
                                            ProjectExistsError)
from upload_rest_api.models.resource import Resource
from upload_rest_api.models.task import Task, TaskEntry, TaskStatus
from upload_rest_api.models.token import Token, TokenEntry, TokenInvalidError
from upload_rest_api.models.upload import (InsufficientQuotaError,
                                           InvalidArchiveError, Upload,
                                           UploadConflictError, UploadEntry,
                                           UploadError, UploadType)
from upload_rest_api.models.user import (User, UserEntry, UserExistsError,
                                         hash_passwd)

__all__ = (
    "FileEntry", "Project", "ProjectEntry", "ProjectExistsError", "Resource",
    "Task", "TaskEntry", "TaskStatus", "Token", "TokenEntry",
    "TokenInvalidError", "InsufficientQuotaError", "InvalidArchiveError",
    "Upload", "UploadConflictError", "UploadEntry", "UploadError",
    "UploadType", "User", "UserEntry", "UserExistsError"
)

try:
    connect(
        host=f"mongodb://{CONFIG['MONGO_HOST']}:{CONFIG['MONGO_PORT']}/upload",
        tz_aware=True,
        # Connect on first operation instead of instantly.
        # This is to prevent MongoClient from being created before a fork,
        # which leads to unexpected behavior.
        connect=False
    )
except KeyError:
    logging.error(
        "MongoDB configuration missing, database connection not configured!"
    )
