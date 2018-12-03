"""Unit tests for module authentication"""
import upload_rest_api.authentication as auth


def test_slow_equals():
    """Test _slow equals() function"""
    assert auth._slow_equals("test", "test")
    assert not auth._slow_equals("test1", "test")
    assert auth._slow_equals("", "")
    assert not auth._slow_equals("", "test")


def test_auth_user(user):
    """Test _auth_user() function
    """
    user.create(password="test")
    assert auth._auth_user("test_user", "test", user=user)
