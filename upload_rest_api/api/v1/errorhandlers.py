"""API v1 error handlers"""
from flask import jsonify


def http_error_generic(error):
    """Generic HTTP error handler"""
    response = jsonify({"code": error.code, "error": str(error)})
    response.status_code = error.code
    return response


def http_error_500(error):
    """Error handler for status code 500"""
    response = jsonify({"code": 500, "error": "Internal server error"})
    response.status_code = 500
    return response
