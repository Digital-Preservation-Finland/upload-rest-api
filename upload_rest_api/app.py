"""REST api for uploading files into passipservice
"""
from flask import Flask, jsonify

import upload_rest_api.authentication as auth


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

    from upload_rest_api.api.v1.files import FILES_API_V1
    from upload_rest_api.api.v1.db import DB_API_V1
    from upload_rest_api.api.v1.metadata import METADATA_API_V1
    app.register_blueprint(FILES_API_V1)
    app.register_blueprint(DB_API_V1)
    app.register_blueprint(METADATA_API_V1)

    @app.errorhandler(401)
    def http_error_401(error):
        """Response handler for status code 401"""
        response = jsonify({"code": error.code, "error": str(error)})
        response.status_code = error.code
        return response


    @app.errorhandler(404)
    def http_error_404(error):
        """Response handler for status code 404"""
        response = jsonify({"code": error.code, "error": str(error)})
        response.status_code = error.code
        return response


    @app.errorhandler(405)
    def http_error_405(error):
        """Response handler for status code 405"""
        response = jsonify({"code": error.code, "error": str(error)})
        response.status_code = error.code
        return response


    @app.errorhandler(409)
    def http_error_409(error):
        """Response handler for status code 409"""
        response = jsonify({"code": error.code, "error": str(error)})
        response.status_code = error.code
        return response


    @app.errorhandler(413)
    def http_error_413(error):
        """Response handler for status code 413"""
        response = jsonify({"code": error.code, "error": str(error)})
        response.status_code = error.code
        return response


    @app.errorhandler(500)
    def http_error_500(error):
        """Response handler for status code 500"""
        response = jsonify({"code": "500", "error": "Internal server error"})
        response.status_code = 500
        return response


    return app


if __name__ == "__main__":
    create_app().run()
