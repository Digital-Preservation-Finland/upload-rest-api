"""Module containing configuration options that will be loaded by a RQ
worker.
"""
from upload_rest_api.config import CONFIG


REDIS_HOST = CONFIG.get("REDIS_HOST", "localhost")
REDIS_PORT = CONFIG.get("REDIS_PORT", 6379)
REDIS_DB = CONFIG.get("REDIS_DB", 0)
REDIS_PASSWORD = CONFIG.get("REDIS_PASSWORD", "")
