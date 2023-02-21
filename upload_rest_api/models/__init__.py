"""upload-rest-api models"""
import logging

from mongoengine import connect

from upload_rest_api.config import CONFIG


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
