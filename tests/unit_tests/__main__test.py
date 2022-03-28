"""Tests for ``upload_rest_api.__main__`` module."""
import datetime
import json

import mock
import pytest
from click.testing import CliRunner

import upload_rest_api.__main__
import upload_rest_api.database as db


@pytest.fixture(scope="function")
def command_runner():
    """Run the CLI entrypoint using the provided arguments and return the
    result.
    """

    def wrapper(args, **kwargs):
        """Run the CLI entrypoint using provided arguments and return
        the result.
        """
        runner = CliRunner()

        result = runner.invoke(
            upload_rest_api.__main__.cli, args, catch_exceptions=False,
            **kwargs
        )
        return result

    return wrapper


@mock.patch('upload_rest_api.__main__.clean_mongo')
@mock.patch('upload_rest_api.__main__.clean_disk')
@pytest.mark.parametrize("command", ("files", "mongo"))
def test_cleanup(mock_clean_disk, mock_clean_mongo, command, command_runner):
    """Test that correct function is called from main function when
    cleanup command is used.
    """
    command_runner(["cleanup", command])

    if command == "files":
        mock_clean_disk.assert_called()
        mock_clean_mongo.assert_not_called()
    else:
        mock_clean_disk.assert_not_called()
        mock_clean_mongo.assert_called()


def test_cleanup_tokens(database, command_runner):
    """
    Test cleaning session tokens using the CLI command
    """
    # Create 3 tokens with varying expiration dates. All but the last one
    # are expired and should be cleaned by this script.
    now = datetime.datetime.now(datetime.timezone.utc)
    token_entries = [
        ("Token 1", now - datetime.timedelta(seconds=30)),
        ("Token 2", now - datetime.timedelta(seconds=0)),
        ("Token 3", now + datetime.timedelta(seconds=30)),  # Not expired
    ]

    for name, expiration_date in token_entries:
        database.tokens.create(
            name=name,
            username="test",
            projects=[],
            expiration_date=expiration_date,
            session=True
        )

    # Only the last token exists
    result = command_runner(["cleanup", "tokens"])
    assert result.output == "Cleaned 2 expired token(s)\n"

    assert database.tokens.tokens.count() == 1
    token = next(database.tokens.tokens.find())
    assert token["name"] == "Token 3"


@pytest.mark.usefixtures('test_mongo')
def test_list_users(command_runner):
    """Test listing all users."""
    database = db.Database()
    database.user("test1").create(projects=["test_project"])
    database.user("test2").create(projects=["test_project"])

    result = command_runner(["users", "list"])
    assert result.output == "test1\ntest2\n"


@pytest.mark.usefixtures('test_mongo')
def test_list_users_when_no_users(command_runner, database):
    """Test listing all users when there are no users."""
    result = command_runner(["users", "list"])

    assert result.output == "No users found\n"


def test_get_user(command_runner):
    """Test displaying information of one user."""
    database = db.Database()
    database.user("test1").create(projects=["test_project"])

    result = command_runner(["users", "get", "test1"])
    result_data = json.loads(result.output)
    correct_result = {
        "_id": "test1",
        "projects": [
            "test_project"
        ]
    }
    assert result_data == correct_result


def test_get_nonexistent_user(command_runner):
    """Test displaying information of a user that does not exist."""
    result = command_runner(["users", "get", "nonexistent_user"])
    assert result.output == "User 'nonexistent_user' not found\n"


@pytest.mark.usefixtures('test_mongo')
def test_list_projects(command_runner, database):
    """Test listing all projects."""
    database.projects.create("test_project_o")
    database.projects.create("test_project_q")
    database.projects.create("test_project_r")

    result = command_runner(["projects", "list"])

    assert "test_project_o\ntest_project_q\ntest_project_r" in result.output


@pytest.mark.usefixtures('test_mongo')
def test_list_projects_when_no_projects(command_runner, database):
    """Test listing all projects when there are no projects."""
    result = command_runner(["projects", "list"])

    assert result.output == "No projects found\n"


@pytest.mark.usefixtures('test_mongo')
def test_get_project(command_runner, database):
    """Test getting information of one project."""
    database.projects.create("test_project_a", quota=1248)

    # Existing project
    result = command_runner(["projects", "get", "test_project_a"])

    data = json.loads(result.output)
    assert data == {
        "_id": "test_project_a",
        "used_quota": 0,
        "quota": 1248
    }

    # Project not found
    result = command_runner(["projects", "get", "test_project_b"])

    assert "Project 'test_project_b' not found" in result.output


def test_create_user(test_mongo, mock_config, command_runner):
    """Test creating user test.

    User should be added to database and project directory should be
    created.
    """
    command_runner(["users", "create", "test_user"])

    assert test_mongo.upload.users.count({"_id": "test_user"}) == 1


@pytest.mark.usefixtures('test_mongo')
def test_create_existing_user(command_runner):
    """Test that creating a user that already exists raises
    UserExistsError.
    """
    db.Database().user("test").create(projects=["test_project"])
    with pytest.raises(db.UserExistsError):
        command_runner(["users", "create", "test"])


def test_delete_user(test_mongo, command_runner):
    """Test deletion of an existing user."""
    db.Database().user("test").create(projects=["test_project"])
    command_runner(["users", "delete", "test"])

    assert test_mongo.upload.users.count({"_id": "test"}) == 0


@pytest.mark.usefixtures('test_mongo')
def test_delete_user_fail(command_runner):
    """Test deletion of an user that does not exist."""
    with pytest.raises(db.UserNotFoundError):
        command_runner(["users", "delete", "test"])


@pytest.mark.usefixtures('test_mongo')
def test_modify_user(command_runner):
    """Test generating a new password for a user."""
    old_password = db.Database().user("test").create()

    user = db.Database().user("test").get()
    old_salt = user["salt"]
    old_digest = user["digest"]

    response = command_runner([
        "users", "modify", "test", "--generate-password"
    ])

    # Assert that password has actually changed
    user = db.Database().user("test").get()
    assert user["salt"] != old_salt
    assert user["digest"] != old_digest

    # Assert that output contains new password
    data = json.loads(response.output)
    assert data["password"]
    assert data["password"] != old_password


@pytest.mark.usefixtures('test_mongo')
def test_modify_user_fail(command_runner):
    """Test modifying a user that does not exist."""
    with pytest.raises(db.UserNotFoundError):
        command_runner(["users", "modify", "test"])


@pytest.mark.usefixtures('test_mongo')
def test_grant_user_projects(database, command_runner):
    """Test granting the user access to projects."""
    user = db.Database().user("test")
    user.create(projects=["test_project"])

    database.projects.create("test_project_2", 2000)
    database.projects.create("test_project_3", 2000)

    result = command_runner([
        "users", "project-rights", "grant", "test", "test_project_2",
        "test_project_3"
    ])

    assert user.get_projects() == [
        "test_project", "test_project_2", "test_project_3"
    ]
    assert result.output == (
        "Granted user 'test' access to project(s): "
        "test_project_2, test_project_3\n"
    )


@pytest.mark.usefixtures('test_mongo')
def test_grant_user_projects_nonexistent_project(database, command_runner):
    """Test granting the user access to project that does not exist."""
    user = db.Database().user("test")
    user.create(projects=["test_project"])

    with pytest.raises(db.ProjectNotFoundError) as exc:
        command_runner([
            "users", "project-rights", "grant", "test", "test_project_2"
        ])

    assert str(exc.value) == "Project 'test_project_2' not found"


@pytest.mark.usefixtures('test_mongo')
def test_grant_user_projects_nonexistent_user(
        database, monkeypatch, command_runner):
    """Test granting a nonexistent user access to a project."""
    database.projects.create("test_project")

    with pytest.raises(db.UserNotFoundError) as exc:
        command_runner([
            "users", "project-rights", "grant", "fake_user", "test_project"
        ])

    assert str(exc.value) == "User 'fake_user' not found"


@pytest.mark.usefixtures('test_mongo')
def test_revoke_user_projects(database, command_runner):
    """Test revoking the user access to projects."""
    user = db.Database().user("test")
    user.create(projects=["test_project"])

    result = command_runner([
        "users", "project-rights", "revoke", "test", "test_project"
    ])

    assert user.get_projects() == []
    assert result.output == (
        "Revoked user 'test' access to project(s): test_project\n"
    )


@pytest.mark.parametrize("quota", [0, 2468])
@pytest.mark.usefixtures("test_mongo")
def test_create_project(database, command_runner, quota):
    """Test creating a new project."""
    command_runner(["projects", "create", "test_project", "--quota", quota])

    project = database.projects.get("test_project")
    assert project["quota"] == quota
    assert project["used_quota"] == 0


def test_create_project_with_negative_quota(command_runner):
    """Test that creating a new project with negative quota results in
    error.
    """
    result = command_runner([
        "projects", "create", "test_project", "--quota", "-1"
    ])

    assert result.exit_code != 0
    assert "Invalid value for '--quota'" in result.output


@pytest.mark.usefixtures("test_mongo")
def test_create_project_already_exists(database, command_runner):
    """Test creating a project that already exists."""
    database.projects.create("test_project", quota=2048)

    with pytest.raises(db.ProjectExistsError) as exc:
        command_runner([
            "projects", "create", "test_project", "--quota", "2048"
        ])

    assert str(exc.value) == "Project 'test_project' already exists"


@pytest.mark.usefixtures("test_mongo")
def test_delete_project(database, command_runner):
    """Test deleting a project"""
    database.projects.create("test_project", quota=2048)

    command_runner(["projects", "delete", "test_project"])

    assert not database.projects.get("test_project")


@pytest.mark.parametrize("quota", [0, 1])
@pytest.mark.usefixtures("test_mongo")
def test_modify_project(command_runner, quota):
    """Test setting new quota for a project"""
    db.Database().projects.create("test_project", quota=2048)

    result = command_runner([
        "projects", "modify", "test_project", "--quota", quota
    ])

    # Assert that quota has actually changed
    project = db.Database().projects.get("test_project")
    assert project["quota"] == quota

    # Assert that output tells the new quota
    data = json.loads(result.output)
    assert data["quota"] == quota


def test_modify_project_with_negative_quota(command_runner):
    """Test that modifying a project with negative quota results in error."""
    result = command_runner([
        "projects", "modify", "test_project", "--quota", "-1"
    ])

    assert result.exit_code != 0
    assert "Invalid value for '--quota'" in result.output


@pytest.mark.usefixtures("test_mongo")
def test_modify_project_fail(command_runner):
    """Test modifying a project that does not exist"""
    result = command_runner(["projects", "modify", "test_project"])

    assert result.output == "Project 'test_project' does not exist.\n"


def test_get_file_by_path(command_runner, database):
    """Test displaying information of file specified by path."""
    database.files.insert_one({"_id": "pid:urn:1", "file_path": "path_1"})
    database.checksums.insert_one("path_1", "checksum_1")

    result = command_runner(["files", "get", "path", "path_1"])
    result_data = json.loads(result.output)
    correct_result = {
        "_id": "pid:urn:1",
        "checksum": "checksum_1",
        "file_path": "path_1"
    }
    assert result_data == correct_result


def test_get_file_by_identifier(command_runner, database):
    """Test displaying information of file specified by identifier."""
    database.files.insert_one({"_id": "pid:urn:1", "file_path": "path_1"})
    database.checksums.insert_one("path_1", "checksum_1")

    result = command_runner(["files", "get", "identifier", "pid:urn:1"])
    result_data = json.loads(result.output)
    correct_result = {
        "_id": "pid:urn:1",
        "checksum": "checksum_1",
        "file_path": "path_1"
    }
    assert result_data == correct_result


def test_list_files(database, command_runner):
    """Test listing all files."""
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

    result = command_runner(["files", "list"])
    correct_result = [
        {"_id": "pid:urn:1", "checksum": "checksum_1", "file_path": "path_1"},
        {"_id": "pid:urn:2", "checksum": "checksum_2", "file_path": "path_2"},
    ]
    result_data = json.loads(result.output)
    assert result_data == correct_result


def test_list_file_identifiers(database, command_runner):
    """Test listing all file identifiers."""
    files = [
        {"_id": "pid:urn:1", "file_path": "path_1"},
        {"_id": "pid:urn:2", "file_path": "path_2"}
    ]
    database.files.insert(files)

    result = command_runner(["files", "list", "--identifiers-only"])
    assert result.output == "pid:urn:1\npid:urn:2\n"


def test_list_checksums(database, command_runner):
    """Test listing all file checksums."""
    checksums = [
        {"_id": "path_1", "checksum": "checksum_1"},
        {"_id": "path_2", "checksum": "checksum_2"}
    ]
    database.checksums.insert(checksums)

    result = command_runner(["files", "list", "--checksums-only"])
    correct_result = {
        "path_1": "checksum_1",
        "path_2": "checksum_2"
    }
    result_data = json.loads(result.output)
    assert result_data == correct_result


def test_get_nonexistent_file_by_path(command_runner):
    """Test displaying information of file specified by path that cannot be
    found in the database.
    """
    result = command_runner(["files", "get", "path", "nonexistent"])
    assert result.output == "File not found in path 'nonexistent'\n"


def test_get_nonexistent_file_by_identifier(command_runner):
    """Test displaying information of file specified by identifier that cannot
    be found in the database.
    """
    result = command_runner(["files", "get", "identifier", "pid:urn:1"])
    assert result.output == "File 'pid:urn:1' not found\n"


def test_list_files_when_no_files(command_runner):
    """Test listing all files when there are no files."""
    result = command_runner(["files", "list"])
    assert result.output == "No files found\n"


def test_list_file_identifiers_when_no_identifiers(command_runner):
    """Test listing all file identifiers when there are no identifers."""
    result = command_runner(["files", "list", "--identifiers-only"])
    assert result.output == "No identifiers found\n"


def test_list_checksums_when_no_checksums(command_runner):
    """Test listing all file checksums when there are no checksums."""
    result = command_runner(["files", "list", "--checksums-only"])
    assert result.output == "No checksums found\n"
