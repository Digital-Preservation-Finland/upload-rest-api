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


def upload_error(error):
    """Create HTTP 400 Bad Request error."""
    return make_response(400, error.message)


def upload_conflict(error):
    """Create HTTP 409 Conflict error.

    The error should contain list of conflicting files.
    """
    response = jsonify({'error': error.message, 'files': error.files})
    response.status_code = 409

    return response


def insufficient_quota(error):
    """Create HTTP 413 Conflict error."""
    return make_response(413, error.message)


def http_error_locked(error):
    """Create HTTP 409 Conflict error indicating the resource is locked."""
    current_app.logger.error(error, exc_info=True)

    message = "The file/directory is currently locked by another task"

    return make_response(409, message)


def http_error_500(error):
    """Create HTTP 500 Internal Server Error response."""
    current_app.logger.error(error, exc_info=True)
    return make_response(500, "Internal server error")
