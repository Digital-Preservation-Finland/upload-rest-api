"""Unit tests for database module"""
import upload_rest_api.database as db


def test_create_user(user):
    """Test creation of of new user
    """
    db = user.users
    user.username = "test"
    user.create()

    assert db.find_one({"_id": "test"}) is not None
