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

    # Set defaults for the different directories under spool
    spool_path = config.get("UPLOAD_BASE_PATH", "/var/spool/upload")

    # 'projects' contains uploaded project files
    config.setdefault(
        "UPLOAD_PROJECTS_PATH", os.path.join(spool_path, "projects")
    )
    # 'tmp' contains temporary storage for archive files
    config.setdefault("UPLOAD_TMP_PATH", os.path.join(spool_path, "tmp"))
    # 'trash' contains temporary storage for files to delete
    config.setdefault("UPLOAD_TRASH_PATH", os.path.join(spool_path, "trash"))
    # 'tus' contains workspaces for files being uploaded using the tus API
    config.setdefault("TUS_API_SPOOL_PATH", os.path.join(spool_path, "tus"))

    return config


CONFIG = get_config()
