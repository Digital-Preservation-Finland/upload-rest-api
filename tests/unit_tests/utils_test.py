"""Unit tests for utility functions."""
import pytest

from upload_rest_api.utils import parse_user_path


@pytest.mark.parametrize(
    "path,result",
    [
        # Valid
        ("test", "/test"),
        ("test/../taste", "/taste"),
        ("test/./././", "/test"),
        ("test/test2", "/test/test2"),
        ("test/../test/../test", "/test"),
        ("äö/😂", "/äö/😂"),
        ("🐸/🐸🐸/🐸🐸🐸/..", "/🐸/🐸🐸"),

        # Invalid
        ("/test", None),
        ("a/b/c/d/../../../../../b", None),
        ("/../../etc/passwd", None),
        ("/test/test/../../../a", None),
        ("/////../test", None)
    ]
)
def test_parse_user_path(path, result):
    """Test valid and invalid user paths.

    Ensure valid paths result in the given absolute path, while invalid
    paths raise an exception.
    """
    if result is not None:
        assert str(parse_user_path("/projects/a", path)) \
            == f"/projects/a{result}"
    else:
        with pytest.raises(ValueError):
            parse_user_path("/projects/a", path)
