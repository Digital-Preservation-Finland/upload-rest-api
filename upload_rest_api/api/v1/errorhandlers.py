"""API v1 error handlers."""
from flask import current_app, jsonify
import werkzeug


def make_response(status_code, message):
    """Return jsonified default error message."""
    response = jsonify({"code": status_code, "error": message})
    response.status_code = status_code
    return response


def http_error_generic(error):
    """Create Generic HTTP error response."""
    current_app.logger.error(error, exc_info=True)
    return make_response(error.code, error.description)


def http_error_404(error):
    """Create HTTP 404 Not Found response."""
    current_app.logger.error(error, exc_info=True)

    if error.description == werkzeug.exceptions.NotFound.description:
        # Replace the default NotFound error description
        message = "Page not found"
    else:
        message = error.description

    return make_response(404, message)


def http_error_500(error):
    """Create HTTP 500 Internal Server Error response."""
    current_app.logger.error(error, exc_info=True)
    return make_response(500, "Internal server error")
