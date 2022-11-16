"""Unit tests for resource module."""
import pytest

from upload_rest_api.models.resource import Resource
from upload_rest_api.security import InvalidPathError


@pytest.mark.parametrize(
    "path,result",
    [
        # Valid
        ("/", "/"),
        ("/test", "/test"),
        ("test", "/test"),
        ("/test/../taste", "/taste"),
        ("/test/../test/../test", "/test"),
        ("/äö/😂", "/äö/😂"),
        ("/🐸/🐸🐸/🐸🐸🐸/..", "/🐸/🐸🐸"),
        ("/test/..", "/"),

        # Invalid
        ("../test", None),
        ("/test/../../", None),
    ]
)
def test_parse_relative_user_path(path, result):
    """Test valid and invalid user provided relative paths.

    Ensure valid paths result in the given relative path, while invalid
    paths raise an exception.
    """
    if result is not None:
        assert str(Resource('test_project', path).path) == result
    else:
        with pytest.raises(InvalidPathError):
            Resource('test_project', path)
