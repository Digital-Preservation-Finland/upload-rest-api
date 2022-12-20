"""REST api for uploading files into passipservice."""
import logging
import sys

from flask import Flask
from werkzeug.exceptions import HTTPException

import upload_rest_api.authentication as auth
from upload_rest_api.api.v1 import files_tus
from upload_rest_api.api.v1.archives import ARCHIVES_API_V1
from upload_rest_api.api.v1.datasets import DATASETS_API_V1
from upload_rest_api.api.v1.directories import DIRECTORIES_API_V1
from upload_rest_api.api.v1.errorhandlers import (http_error_400,
                                                  http_error_404,
                                                  http_error_500,
                                                  http_error_generic,
                                                  http_error_locked,
                                                  insufficient_quota,
                                                  upload_conflict)
from upload_rest_api.api.v1.files import FILES_API_V1
from upload_rest_api.api.v1.tasks import TASK_STATUS_API_V1
from upload_rest_api.api.v1.tokens import TOKEN_API_V1
from upload_rest_api.api.v1.users import USERS_API_V1
from upload_rest_api.config import get_config
from upload_rest_api.lock import LockAlreadyTaken
from upload_rest_api.models.upload import (InsufficientQuotaError,
                                           UploadConflictError, UploadError)
from upload_rest_api.security import InvalidPathError

try:
    # Newer Werkzeug
    from werkzeug.middleware.proxy_fix import ProxyFix
except ImportError:
    # Older Werkzeug
    from werkzeug.contrib.fixers import ProxyFix


logging.basicConfig(level=logging.ERROR)
LOGGER = logging.getLogger(__name__)


def configure_app(app):
    """Read config from /etc/upload_rest_api.conf."""
    # Require that "/etc/upload_rest_api.conf" exists
    app.config.from_pyfile("/etc/upload_rest_api.conf")

    app.config.update(get_config())


def create_app():
    """Configure and return a Flask application instance.

    :returns: Instance of flask.Flask()
    """
    if sys.getfilesystemencoding() != "utf-8":
        # If detected filesystem encoding is incorrect, halt immediately.
        # Wrong file system encoding will cause file names on disk to be
        # handled incorrectly.
        raise OSError(
            f"Expected file system encoding to be 'utf-8', "
            f"found {sys.getfilesystemencoding()} instead."
        )

    app = Flask(__name__)

    try:
        # Newer Werkzeug requires explicitly defining the HTTP headers
        # and the number of proxies handling each header
        app.wsgi_app = ProxyFix(
            app.wsgi_app,
            # X-Forwarded-For, X-Forwarded-Host and X-Forwarded-Server
            # are set by mod_proxy
            x_for=1,
            x_host=1
        )
    except TypeError:
        app.wsgi_app = ProxyFix(app.wsgi_app, num_proxies=1)

    # Configure app
    configure_app(app)

    # Authenticate all requests
    app.before_request(auth.authenticate)

    # Register all blueprints
    app.register_blueprint(DATASETS_API_V1)
    app.register_blueprint(DIRECTORIES_API_V1)
    app.register_blueprint(FILES_API_V1)
    app.register_blueprint(ARCHIVES_API_V1)
    app.register_blueprint(TASK_STATUS_API_V1)
    app.register_blueprint(TOKEN_API_V1)
    app.register_blueprint(USERS_API_V1)

    files_tus.register_blueprint(app)

    # Register error handlers
    app.register_error_handler(HTTPException, http_error_generic)
    app.register_error_handler(404, http_error_404)
    app.register_error_handler(500, http_error_500)
    app.register_error_handler(LockAlreadyTaken, http_error_locked)
    app.register_error_handler(UploadConflictError, upload_conflict)
    app.register_error_handler(InsufficientQuotaError, insufficient_quota)
    app.register_error_handler(UploadError, http_error_400)
    app.register_error_handler(InvalidPathError, http_error_400)

    return app


if __name__ == "__main__":
    create_app().run(debug=True)
