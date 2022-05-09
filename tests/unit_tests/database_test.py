"""Unit tests for database module."""
import binascii
import pathlib
import re

import bson
import pytest

import upload_rest_api.database as db


def test_dir_size():
    """Test that dir sizes are calculated correctly.

    Dirs that do not exist should return size 0.
    """
    # Existing dir
    assert db.get_dir_size("tests/data/get_dir_size") == 8

    # Non-existent dir
    assert db.get_dir_size("tests/data/test") == 0


@pytest.mark.parametrize('projects', [None,
                                      [],
                                      ['test_project'],
                                      ['project1', 'project2']])
def test_create_user(database, projects):
    """Test creation of new user.

    :param database: Database instance
    :param projects: List of user projects
    """
    user = database.user('test_user')
    user.create(projects=projects)
    assert user.exists()

    user_dict = user.get()
    assert len(user_dict["salt"]) == 20
    assert len(user_dict["digest"]) == 64

    if projects:
        assert user_dict["projects"] == projects
    else:
        assert user_dict["projects"] == []


def test_create_project(database, mock_config):
    """Test creating new project."""
    database.projects.create("test_project")

    project_dict = database.projects.projects.find_one({"_id": "test_project"})

    assert project_dict["quota"] == 5 * 1024**3
    assert project_dict["used_quota"] == 0

    # Project directory should be created
    project_directory \
        = pathlib.Path(mock_config["UPLOAD_PROJECTS_PATH"]) / "test_project"
    assert project_directory.is_dir()


def test_create_two_users(user):
    """Test creating two users with same project."""
    users = user.users

    user.username = "test_user1"
    user.create(projects=["test_project"])

    user.username = "test_user2"
    user.create(projects=["test_project"])

    user1_project = users.find_one({"_id": "test_user1"})["projects"][0]
    user2_project = users.find_one({"_id": "test_user2"})["projects"][0]

    assert user1_project == user2_project == "test_project"


def test_delete_user(user):
    """Test deletion of user."""
    users = user.users

    users.insert_one({"_id": "test_user"})
    users.insert_one({"_id": "test_user2"})

    user.delete()

    assert users.find_one({"_id": "test_user"}) is None
    assert users.find_one({"_id": "test_user2"}) is not None


def test_get_all_ids(files_col):
    """Test get_all_ids returns a list of all _ids in the collection."""
    assert files_col.get_all_ids() == []

    for i in range(10):
        files_col.files.insert_one(
            {"_id": f"pid:urn:{i}", "file_path": str(i)}
        )
        assert files_col.get_all_ids() == [
            f"pid:urn:{j}" for j in range(i+1)
        ]


def test_insert_and_delete_files(files_col):
    """Test insertion and deletion of files documents."""
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


def test_store_identifiers(monkeypatch):
    """Test store_identifiers method.

    Test that store_identifiers writes the POSTed identifiers and
    corresponding file_paths to Mongo.
    """
    monkeypatch.setattr(db, "_get_abs_path",
                        lambda path, _root_path, _project: path)

    metax_response = [
        {"object": {"identifier": "pid:urn:1", "file_path": "1"}},
        {"object": {"identifier": "pid:urn:2", "file_path": "2"}},
        {"object": {"identifier": "pid:urn:3", "file_path": "3"}}
    ]

    database = db.Database()
    database.store_identifiers(metax_response, "/tmp", "project_path")
    assert database.files.get_all_ids() \
        == ["pid:urn:1", "pid:urn:2", "pid:urn:3"]


def test_quota(database):
    """Test set_used_quota() and set_quota() functions."""
    database.projects.create("test_project")
    project = database.projects.get("test_project")

    assert project["quota"] == 5 * 1024**3
    assert project["used_quota"] == 0

    # Set
    database.projects.set_quota("test_project", 1024)
    database.projects.set_used_quota("test_project", 512)

    # Get
    project = database.projects.get("test_project")
    assert project["quota"] == 1024
    assert project["used_quota"] == 512


def test_get_random_string():
    """Test get_random_string method.

    Test that get_random_string() returns random strings
    of given lenght with only ascii letters and digits.
    """
    strings = set()

    for _ in range(1000):
        string = db.get_random_string(20)

        assert len(string) == 20
        assert re.match("^[A-Za-z0-9_-]*$", string)
        assert string not in strings
        strings.add(string)


def test_hash_passwd():
    """Test that salting and hashing returns the correct digest."""
    digest = binascii.hexlify(db.hash_passwd("test", "test")[:16])
    assert digest == b"4b119f6da6890ed1cc68d5b3adf7d053"


def test_async_task_creation(tasks_col):
    """Test creation of tasks documents."""
    task_id_1 = tasks_col.create("test_project")
    task_id_2 = tasks_col.create("test_project")
    assert task_id_1 != task_id_2
    assert len(tasks_col.find("test_project", "pending")) == 2


def test_async_task_update(tasks_col):
    """Test update of tasks documents."""
    task_id_1 = tasks_col.create("test_project")
    tasks_col.update_status(task_id_1, "done")
    assert len(tasks_col.find("test_project", "done")) == 1
    assert len(tasks_col.find("test_project", "pending")) == 0

    task = tasks_col.get(task_id_1)
    assert task["status"] == "done"
    assert "message" not in task
    tasks_col.update_message(task_id_1, "Message")
    task = tasks_col.get(task_id_1)
    assert task["status"] == "done"
    assert task["message"] == "Message"


def test_async_task_delete(tasks_col):
    """Test deletion of tasks documents."""
    task_id_1 = tasks_col.create("test_user_1")
    task_id_2 = tasks_col.create("test_user_2")
    assert tasks_col.delete_one(task_id_1) == 1
    assert tasks_col.delete_one(task_id_2) == 1
    assert tasks_col.delete_one(task_id_1) == 0
    assert tasks_col.delete_one(task_id_2) == 0
    assert tasks_col.delete([task_id_2, task_id_2]) == 0

    task_id_1 = tasks_col.create("test_user_1")
    task_id_2 = tasks_col.create("test_user_2")
    assert tasks_col.delete([task_id_1, task_id_2]) == 2


@pytest.mark.parametrize('method',
                         ['update_status', 'update_message', 'update_error'])
def test_task_not_found(tasks_col, method):
    """Test that error is raised if task is not found.

    :param tasks_col: Tasks object
    :param method: method to be tested
    """
    # Create valid random identifier
    task_identifier = bson.objectid.ObjectId()

    with pytest.raises(db.TaskNotFoundError, match='Task .* not found'):
        getattr(tasks_col, method)(task_identifier, 'bar')


def test_checksums_delete_chunks(checksums_col):
    """Test deleting a large amount of checksums.

    The deletion queries are split into chunks internally to prevent
    exceeding MongoDB's query size limit.
    """
    # 20,100 checksums will be added
    for i in range(0, 201):
        checksums_col.insert([
            {"_id": f"/path/{(i*100)+j}", "checksum": "foobar"}
            for j in range(0, 100)
        ])

    assert checksums_col.checksums.count({}) == 20100

    # Delete all but the last 3 checksum entries using `delete`
    paths_to_delete = [f"/path/{i}" for i in range(0, 20097)]
    assert checksums_col.delete(paths_to_delete) == 20097

    # 3 checksums are left
    assert checksums_col.get_checksums() == {
        "/path/20097": "foobar",
        "/path/20098": "foobar",
        "/path/20099": "foobar"
    }


def test_get_all_files_with_checksums(database):
    """Test getting all files with corresponding checksums."""
    files = [
        {"_id": "pid:urn:1", "file_path": "path_1"},
        {"_id": "pid:urn:2", "file_path": "path_2"}
    ]
    checksums = [
        {"_id": "path_1", "checksum": "checksum_1"},
        {"_id": "path_2", "checksum": "checksum_2"}
    ]
    database.files.insert(files)
    database.checksums.insert(checksums)

    files_with_checksums = database.files.get_all_files_with_checksums()
    correct_result = [
        {"_id": "pid:urn:1", "checksum": "checksum_1", "file_path": "path_1"},
        {"_id": "pid:urn:2", "checksum": "checksum_2", "file_path": "path_2"},
    ]
    assert files_with_checksums == correct_result


def test_get_all_files(database):
    """Test getting all files."""
    files = [
        {"_id": "pid:urn:1", "file_path": "path_1"},
        {"_id": "pid:urn:2", "file_path": "path_2"}
    ]
    database.files.insert(files)
    received_files = database.files.get_all_files()
    assert received_files == files


def test_deleting_files_by_path(database):
    """Test deleting files with given paths."""
    files = [
        {"_id": "pid:urn:1", "file_path": "path_1"},
        {"_id": "pid:urn:2", "file_path": "path_2"},
        {"_id": "pid:urn:3", "file_path": "path_3"}
    ]
    database.files.insert(files)
    database.files.delete_paths(["path_1", "path_2", "path_does_not_exist"])
    files = database.files.get_all_files()
    assert files == [{"_id": "pid:urn:3", "file_path": "path_3"}]
