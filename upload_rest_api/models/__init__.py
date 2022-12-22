"""upload-rest-api models"""
import logging

from mongoengine import connect

from upload_rest_api.config import CONFIG
from upload_rest_api.models.project import Project
from upload_rest_api.models.resource import Resource
from upload_rest_api.models.task import Task
from upload_rest_api.models.token import Token
from upload_rest_api.models.upload import Upload
from upload_rest_api.models.user import User
from upload_rest_api.models.file_entry import FileEntry

__all__ = ("Project", "Resource", "Task", "Token", "Upload", "User",
           "FileEntry")

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
