"""API v1 error handlers"""
from flask import current_app
import upload_rest_api.utils as utils


def http_error_generic(error):
    """Generic HTTP error handler"""
    current_app.logger.error(error, exc_info=True)
    return utils.make_response(error.code, str(error))


def http_error_500(error):
    """Error handler for status code 500"""
    current_app.logger.error(error, exc_info=True)
    return utils.make_response(500, "Internal server error")
