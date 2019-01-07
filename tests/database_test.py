"""Unit tests for database module"""
import re
import binascii

import upload_rest_api.database as db


def test_dir_size():
    """Test that dir sizes are calculated correctly. Dirs that do not
    exist should return size 0.
    """
    # Existing dir
    assert db.get_dir_size("tests/data") == 530

    # Non-existent dir
    assert db.get_dir_size("tests/data/test") == 0


def test_create_user(user):
    """Test creation of new user
    """
    db = user.users
    user.username = "test"
    user.create("test_project")

    user_dict = db.find_one({"_id": "test"})

    assert user_dict is not None
    assert user.exists()
    assert user_dict == user.get()

    salt = user_dict["salt"]
    assert len(salt) == 20

    used_quota = user_dict["used_quota"]
    assert used_quota == 0

    quota = user_dict["quota"]
    assert quota == 5 * 1024**3

    digest = user_dict["digest"]
    assert len(digest) == 64

    project = user_dict["project"]
    assert project == "test_project"


def test_delete_user(user):
    """Test deletion of user
    """
    db = user.users

    db.insert_one({"_id": "test_user"})
    db.insert_one({"_id": "test_user2"})

    user.delete()

    assert db.find_one({"_id": "test_user"}) is None
    assert db.find_one({"_id": "test_user2"}) is not None


def test_quota(user):
    """Test get_quota() and set_quota() functions
    """
    db = user.users
    db.insert_one({"_id": "test_user", "quota": 5 * 1024**3})

    # Get
    assert user.get_quota() == 5 * 1024**3

    # Set
    user.set_quota(0)
    assert db.find_one({"_id": "test_user"})["quota"] == 0


def test_get_random_string():
    """Test that _get_random_string() returns random strings
    of given lenght with only ascii letters and digits
    """
    strings = set()

    for _ in range(1000):
        string = db._get_random_string(20)

        assert len(string) == 20
        assert re.match("^[A-Za-z0-9_-]*$", string)
        assert string not in strings
        strings.add(string)


def test_hash_passwd():
    """Test that salting and hashing returns the correct digest
    """
    digest = binascii.hexlify(db.hash_passwd("test", "test")[:16])
    assert digest == "8809fd1f1e620cf1156353571199e227"
