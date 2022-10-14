"""Database models"""
import logging

from mongoengine import connect

from upload_rest_api.config import CONFIG
from upload_rest_api.models.file import *     # noqa: F403, F401
from upload_rest_api.models.project import *  # noqa: F403, F401
from upload_rest_api.models.task import *     # noqa: F403, F401
from upload_rest_api.models.token import *    # noqa: F403, F401
from upload_rest_api.models.upload import *   # noqa: F403, F401
from upload_rest_api.models.user import *     # noqa: F403, F401

try:
    connect(
        host=f"mongodb://{CONFIG['MONGO_HOST']}:{CONFIG['MONGO_PORT']}/upload",
        tz_aware=True
    )
except KeyError:
    logging.error(
        "MongoDB configuration missing, database connection not configured!"
    )
