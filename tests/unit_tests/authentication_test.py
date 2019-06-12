"""Unit tests for module authentication"""
import upload_rest_api.authentication as auth


def test_auth_user(user):
    """Test _auth_user() function
    """
    user.create("test_project", password="test")
    assert auth._auth_user("test_user", "test", user=user)
