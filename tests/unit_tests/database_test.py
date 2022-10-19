"""Unit tests for database module."""
import binascii
import pathlib
import re

import bson
import pytest

from upload_rest_api.database import (DBFile, Project, User, get_dir_size,
                                      get_random_string, hash_passwd)


def test_dir_size():
    """Test that dir sizes are calculated correctly.

    Dirs that do not exist should return size 0.
    """
    # Existing dir
    assert get_dir_size("tests/data/get_dir_size") == 8

    # Non-existent dir
    assert get_dir_size("tests/data/test") == 0


@pytest.mark.parametrize('projects', [None,
                                      [],
                                      ['test_project'],
                                      ['project1', 'project2']])
def test_create_user(database, projects):
    """Test creation of new user.

    :param database: Database instance
    :param projects: List of user projects
    """
    user = User.create("test_user", projects=projects)

    data = user.to_mongo()
    assert data["_id"] == "test_user"

    assert len(user.salt) == 20
    assert len(user.digest) == 64

    if projects:
        assert user.projects == projects
    else:
        assert user.projects == []


def test_create_project(test_mongo, mock_config):
    """Test creating new project."""
    Project.create("test_project")

    project_dict = test_mongo.upload.projects.find_one({"_id": "test_project"})

    assert project_dict["quota"] == 5 * 1024**3
    assert project_dict["used_quota"] == 0

    # Project directory should be created
    project_directory \
        = pathlib.Path(mock_config["UPLOAD_PROJECTS_PATH"]) / "test_project"
    assert project_directory.is_dir()


def test_get_random_string():
    """Test get_random_string method.

    Test that get_random_string() returns random strings
    of given lenght with only ascii letters and digits.
    """
    strings = set()

    for _ in range(1000):
        string = get_random_string(20)

        assert len(string) == 20
        assert re.match("^[A-Za-z0-9_-]*$", string)
        assert string not in strings
        strings.add(string)


def test_hash_passwd():
    """Test that salting and hashing returns the correct digest."""
    digest = binascii.hexlify(hash_passwd("test", "test")[:16])
    assert digest == b"4b119f6da6890ed1cc68d5b3adf7d053"


def test_files_delete_chunks(test_mongo):
    """Test deleting a large amount of files.

    The deletion queries are split into chunks internally to prevent
    exceeding MongoDB's query size limit.
    """
    # 20,100 files will be added
    for i in range(0, 201):
        test_mongo.upload.files.insert([
            {"_id": f"/path/{(i*100)+j}",
             "checksum": "foobar",
             "identifier": '1'}
            for j in range(0, 100)
        ])

    assert DBFile.objects.count() == 20100

    # Delete all but the last 3 files entries using `delete`
    paths_to_delete = [f"/path/{i}" for i in range(0, 20097)]
    assert DBFile.objects.bulk_delete_by_paths(paths_to_delete) == 20097

    # 3 files are left
    assert DBFile.objects.count() == 3
    assert list(test_mongo.upload.files.find()) == [
        {"_id": "/path/20097", "checksum": "foobar", "identifier": '1'},
        {"_id": "/path/20098", "checksum": "foobar", "identifier": '1'},
        {"_id": "/path/20099", "checksum": "foobar", "identifier": '1'}
    ]


def test_get_path_checksum_dict(test_mongo):
    """Test getting files as dict of file paths and checksums."""
    files = [
        {"_id": "path_1",
         "checksum": "checksum_1",
         "identifier": "pid:urn:1"},
        {"_id": "path_2",
         "checksum": "checksum_2",
         "identifier": "pid:urn:2"}
    ]
    test_mongo.upload.files.insert_many(files)

    correct_result = {"path_1": "checksum_1", "path_2": "checksum_2"}
    assert DBFile.get_path_checksum_dict() == correct_result
