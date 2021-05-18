"""REST api for uploading files into passipservice."""
import logging

import upload_rest_api.authentication as auth
from flask import Flask
from upload_rest_api.api.v1.archives import ARCHIVES_API_V1
from upload_rest_api.api.v1.errorhandlers import (http_error_500,
                                                  http_error_generic)
from upload_rest_api.api.v1.files import FILES_API_V1
from upload_rest_api.api.v1.metadata import METADATA_API_V1
from upload_rest_api.api.v1.tasks import TASK_STATUS_API_V1

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
    app.config.from_pyfile("/etc/upload_rest_api.conf")


def create_app():
    """Configure and return a Flask application instance.

    :returns: Instance of flask.Flask()
    """
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
    app.register_blueprint(FILES_API_V1)
    app.register_blueprint(ARCHIVES_API_V1)
    app.register_blueprint(METADATA_API_V1)
    app.register_blueprint(TASK_STATUS_API_V1)

    # Register error handlers
    for status_code in [400, 401, 404, 405, 409, 413, 415]:
        app.register_error_handler(status_code, http_error_generic)

    app.register_error_handler(500, http_error_500)

    return app


if __name__ == "__main__":
    create_app().run(debug=True)
