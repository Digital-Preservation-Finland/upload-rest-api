"""upload-rest-api models"""
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
from upload_rest_api.models.user import User, UserExistsError, hash_passwd

try:
    connect(
        host=f"mongodb://{CONFIG['MONGO_HOST']}:{CONFIG['MONGO_PORT']}/upload",
        tz_aware=True
    )
except KeyError:
    logging.error(
        "MongoDB configuration missing, database connection not configured!"
    )
