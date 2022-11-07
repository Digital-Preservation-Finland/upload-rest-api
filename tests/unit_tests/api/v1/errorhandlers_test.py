"""Tests for api.v1.errorhandlers module."""
import pytest
import werkzeug.exceptions

from upload_rest_api.lock import LockAlreadyTaken
from upload_rest_api.models import (InsufficientQuotaError,
                                    UploadConflictError, UploadError)


@pytest.mark.parametrize(
    "exception,expected_response,logged_error",
    [
        (
            werkzeug.exceptions.Unauthorized('Foo'),
            {"code": 401, "error": 'Foo'},
            None
        ),
        (
            werkzeug.exceptions.NotFound('Foo'),
            {"code": 404, "error": 'Foo'},
            None
        ),
        (
            werkzeug.exceptions.Conflict('Foo'),
            {"code": 409, "error": 'Foo'},
            None
        ),
        # Server error should be logged
        (
            werkzeug.exceptions.NotImplemented('Foo'),
            {"code": 501, "error": 'Foo'},
            "501 Not Implemented: Foo"
        ),
        # In case of Internal server error, the error should not
        # revealed to user, but the cause of the error should be logged.
        (
            werkzeug.exceptions.InternalServerError('Foo'),
            {"code": 500, "error": 'Internal server error'},
            "500 Internal Server Error: Foo"
        ),
        # The default 404 error message should be "Page not found"
        (
            werkzeug.exceptions.NotFound(),
            {"code": 404, "error": 'Page not found'},
            None
        ),
        (
            UploadError("Foo"),
            {"code": 400, "error": "Foo"},
            None
        ),
        (
            UploadConflictError("Foo", ['file1', 'file2']),
            {"code": 409, "error": "Foo", 'files': ['file1', 'file2']},
            None
        ),
        (
            InsufficientQuotaError("Foo"),
            {"code": 413, "error": "Foo"},
            None
        ),
        (
            LockAlreadyTaken("Foo"),
            {"code": 409, "error": "The file/directory is currently "
                                   "locked by another task"},
            None
        ),
    ]
)
def test_error_handling(app, exception, expected_response, logged_error,
                        test_auth, caplog):
    """Test error handling.

    :param app: Flask app
    :param exception: Exception raised
    :param expected_response: Response sent to user
    :param logged_error: Logged error message
    :param test_auth: Authentication headers
    :param caplog: Captured logs
    """
    # Add a test route that always rises exception
    @app.route('/test')
    def _raise_exception():
        """Raise exception."""
        raise exception

    with app.test_client() as client:
        response = client.get('/test', headers=test_auth)

    assert response.json == expected_response
    assert response.status_code == expected_response['code']

    if logged_error:
        # Server errors should be logged
        assert len(caplog.records) == 1
        assert caplog.records[0].message == logged_error
    else:
        # Nothing should be logged
        assert not caplog.records
