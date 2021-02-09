"""Read configuration file."""
import os.path
import logging
from flask import Config


def get_config():
    """Get the configuration file and return a flask.Config instance."""
    config = Config("/")

    if os.path.exists("/etc/upload_rest_api.conf"):
        config.from_pyfile("/etc/upload_rest_api.conf")
    else:
        logging.error("/etc/upload_rest_api.conf not found!")

    return config


CONFIG = get_config()
