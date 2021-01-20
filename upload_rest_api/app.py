"""REST api for uploading files into passipservice
"""
from __future__ import unicode_literals

import logging

from flask import Flask

import upload_rest_api.authentication as auth


logging.basicConfig(level=logging.ERROR)
LOGGER = logging.getLogger(__name__)


def configure_app(app):
    """Read config from /etc/upload_rest_api.conf"""
    app.config.from_pyfile("/etc/upload_rest_api.conf")


def create_app():
    """Configure and return a Flask application instance.

    :returns: Instance of flask.Flask()
    """
    app = Flask(__name__)

    # Configure app
    configure_app(app)

    # Authenticate all requests
    app.before_request(auth.authenticate)

    # Register all blueprints
    from upload_rest_api.api.v1.files import FILES_API_V1
    from upload_rest_api.api.v1.archives import ARCHIVES_API_V1
    from upload_rest_api.api.v1.metadata import METADATA_API_V1
    from upload_rest_api.api.v1.tasks import TASK_STATUS_API_V1
    app.register_blueprint(FILES_API_V1)
    app.register_blueprint(ARCHIVES_API_V1)
    app.register_blueprint(METADATA_API_V1)
    app.register_blueprint(TASK_STATUS_API_V1)

    # Register error handlers
    from upload_rest_api.api.v1.errorhandlers import http_error_generic
    for status_code in [400, 401, 404, 405, 409, 413, 415]:
        app.register_error_handler(status_code, http_error_generic)

    from upload_rest_api.api.v1.errorhandlers import http_error_500
    app.register_error_handler(500, http_error_500)

    return app


if __name__ == "__main__":
    create_app().run(debug=True)
