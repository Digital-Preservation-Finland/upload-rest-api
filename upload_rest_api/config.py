"""Read configuration file."""
import logging
import os.path

from flask import Config


def configure_logger():
    """
    Configure root logger for the application if it is not already
    configured
    """
    logging.basicConfig(level=logging.INFO)


def get_config():
    """Get the configuration file and return a flask.Config instance."""
    config = Config("/")

    config.from_pyfile("/etc/upload_rest_api.conf")

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
