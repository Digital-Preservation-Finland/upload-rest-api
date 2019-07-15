"""Unit tests for database module"""
from __future__ import unicode_literals

import re
import binascii

import upload_rest_api.database as db


def test_dir_size():
    """Test that dir sizes are calculated correctly. Dirs that do not
    exist should return size 0.
    """
    # Existing dir
    assert db.get_dir_size("tests/data") == 2156

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


def test_get_all_ids(files_col):
    """Test get_all_ids returns a list of all _ids in the collection
    """
    assert files_col.get_all_ids() == []

    for i in range(10):
        files_col.files.insert_one(
            {"_id": "pid:urn:%s" % i, "file_path": "%s" % i}
        )
        assert files_col.get_all_ids() == [
            "pid:urn:%s" % j for j in range(i+1)
        ]


def test_insert_and_delete_files(files_col):
    """Test insertion and deletion of files documents
    """
    document = {"_id": "pid:urn:1", "file_path": "1"}
    many_documents = [
        {"_id": "pid:urn:2", "file_path": "2"},
        {"_id": "pid:urn:3", "file_path": "3"}
    ]

    files_col.insert_one(document)
    assert len(files_col.get_all_ids()) == 1

    files_col.insert(many_documents)
    assert len(files_col.get_all_ids()) == 3

    files_col.delete_one("pid:urn:1")
    assert len(files_col.get_all_ids()) == 2

    files_col.delete(["pid:urn:2", "pid:urn:3"])
    assert len(files_col.get_all_ids()) == 0


def test_store_identifiers(files_col, monkeypatch):
    """Test that store_identifiers writes the POSTed identifiers and
    corresponding file_paths to Mongo.
    """
    monkeypatch.setattr(db, "_get_abs_path", lambda path: path)

    metax_response = [
        {"object": {"identifier": "pid:urn:1", "file_path": "1"}},
        {"object": {"identifier": "pid:urn:2", "file_path": "2"}},
        {"object": {"identifier": "pid:urn:3", "file_path": "3"}}
    ]

    files_col.store_identifiers(metax_response)
    assert files_col.get_all_ids() == ["pid:urn:1", "pid:urn:2", "pid:urn:3"]


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
    """Test that get_random_string() returns random strings
    of given lenght with only ascii letters and digits
    """
    strings = set()

    for _ in range(1000):
        string = db.get_random_string(20)

        assert len(string) == 20
        assert re.match("^[A-Za-z0-9_-]*$", string)
        assert string not in strings
        strings.add(string)


def test_hash_passwd():
    """Test that salting and hashing returns the correct digest
    """
    digest = binascii.hexlify(db.hash_passwd("test", "test")[:16])
    assert digest == b"4b119f6da6890ed1cc68d5b3adf7d053"
