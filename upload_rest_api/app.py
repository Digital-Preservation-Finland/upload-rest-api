"""REST api for uploading files into passipservice
"""
import logging
import logging.handlers

from flask import Flask

import upload_rest_api.authentication as auth


def configure_app(app):
    """Read config from /etc/upload_rest_api.conf"""
    app.config.from_pyfile("/etc/upload_rest_api.conf")


def create_app(testing=False):
    """Configure and return a Flask application instance.

    :returns: Instance of flask.Flask()
    """
    app = Flask(__name__)

    # Configure app
    configure_app(app)

    # Authenticate all requests
    app.before_request(auth.authenticate)

    # Add logger
    if not testing:
        file_handler = logging.handlers.TimedRotatingFileHandler(
            "/var/log/upload_rest_api/upload_rest_api.log",
            when="midnight", backupCount=6
        )
        file_handler.setLevel(logging.WARNING)
        file_handler.setFormatter(
            logging.Formatter("\n[%(asctime)s - %(levelname)s]\n%(message)s")
        )
        app.logger.addHandler(file_handler)

    # Register all blueprints
    from upload_rest_api.api.v1.files import FILES_API_V1
    from upload_rest_api.api.v1.db import DB_API_V1
    from upload_rest_api.api.v1.metadata import METADATA_API_V1
    app.register_blueprint(FILES_API_V1)
    app.register_blueprint(DB_API_V1)
    app.register_blueprint(METADATA_API_V1)

    # Register error handlers
    from upload_rest_api.api.v1.errorhandlers import http_error_generic
    for status_code in [401, 404, 405, 409, 413]:
        app.register_error_handler(status_code, http_error_generic)

    from upload_rest_api.api.v1.errorhandlers import http_error_500
    app.register_error_handler(500, http_error_500)

    return app


if __name__ == "__main__":
    create_app(testing=True).run(debug=True)
