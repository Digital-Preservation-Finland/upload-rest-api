import logging

from mongoengine import connect

from upload_rest_api.config import CONFIG

try:
    connect(
        host=f"mongodb://{CONFIG['MONGO_HOST']}:{CONFIG['MONGO_PORT']}/upload",
        tz_aware=True
    )
except KeyError:
    logging.error(
        "MongoDB configuration missing, database connection not configured!"
    )


from upload_rest_api.models.file import *
from upload_rest_api.models.project import *
from upload_rest_api.models.task import *
from upload_rest_api.models.token import *
from upload_rest_api.models.upload import *
from upload_rest_api.models.user import *
