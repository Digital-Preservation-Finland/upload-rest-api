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
@pytest.mark.parametrize("flag", ("--files", "--mongo"))
def test_cleanup(mock_clean_disk, mock_clean_mongo, flag, command_runner):
    """Test that correct function is called from main function when
    cleanup command is used.
    """
    command_runner(["cleanup", flag])

    if flag == "--files":
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
    result = command_runner(["cleanup", "--tokens"])
    assert result.output == "Cleaned 2 expired token(s)\n"

    assert database.tokens.tokens.count() == 1
    token = next(database.tokens.tokens.find())
    assert token["name"] == "Token 3"


@pytest.mark.usefixtures('test_mongo')
def test_list_users(command_runner):
    """Test `list --users` command."""
    database = db.Database()
    database.user("test1").create(projects=["test_project"])
    database.user("test2").create(projects=["test_project"])

    result = command_runner(["list", "--users"])
    assert result.output == "test1\ntest2\n"


@pytest.mark.usefixtures('test_mongo')
def test_list_users_when_no_users(command_runner, database):
    """Test `list --userss` command when there are no users."""
    result = command_runner(["list", "--users"])

    assert result.output == "No users found\n"


@pytest.mark.usefixtures('test_mongo')
def test_list_projects(command_runner, database):
    """Test `list --projects` command."""
    database.projects.create("test_project_o")
    database.projects.create("test_project_q")
    database.projects.create("test_project_r")

    result = command_runner(["list", "--projects"])

    assert "test_project_o\ntest_project_q\ntest_project_r" in result.output


@pytest.mark.usefixtures('test_mongo')
def test_list_projects_when_no_projects(command_runner, database):
    """Test `list --projects` command when there are no projects."""
    result = command_runner(["list", "--projects"])

    assert result.output == "No projects found\n"


@pytest.mark.usefixtures('test_mongo')
def test_list_project(command_runner, database):
    """Test `list --project <id>` command."""
    database.projects.create("test_project_a", quota=1248)

    # Existing project
    result = command_runner(["list", "--project", "test_project_a"])

    data = json.loads(result.output)
    assert data == {
        "_id": "test_project_a",
        "used_quota": 0,
        "quota": 1248
    }

    # Project not found
    result = command_runner(["list", "--project", "test_project_b"])

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

    response = command_runner(["users", "modify", "test", "--password"])

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
        "users", "project-rights", "--grant", "test", "test_project_2",
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
            "users", "project-rights", "--grant", "test", "test_project_2"
        ])

    assert str(exc.value) == "Project 'test_project_2' not found"


@pytest.mark.usefixtures('test_mongo')
def test_grant_user_projects_nonexistent_user(
        database, monkeypatch, command_runner):
    """Test granting a nonexistent user access to a project."""
    database.projects.create("test_project")

    with pytest.raises(db.UserNotFoundError) as exc:
        command_runner([
            "users", "project-rights", "--grant", "fake_user", "test_project"
        ])

    assert str(exc.value) == "User 'fake_user' not found"


@pytest.mark.usefixtures('test_mongo')
def test_revoke_user_projects(database, command_runner):
    """Test revoking the user access to projects."""
    user = db.Database().user("test")
    user.create(projects=["test_project"])

    result = command_runner([
        "users", "project-rights", "--revoke", "test", "test_project"
    ])

    assert user.get_projects() == []
    assert result.output == (
        "Revoked user 'test' access to project(s): test_project\n"
    )


def test_user_project_rights_with_invalid_flags(command_runner):
    """Test giving user access to projects with confusing commands."""
    # Both grant and revoke access to projects
    result = command_runner([
        "users", "project-rights", "--grant", "--revoke", "user", "project"
    ])
    assert result.exit_code != 0
    assert "Set one and only one of --grant or --revoke." in result.output

    # Don't grant or revoke access to projects
    result = command_runner([
        "users", "project-rights", "user", "project"
    ])
    assert result.exit_code != 0
    assert "Set one and only one of --grant or --revoke." in result.output


@pytest.mark.usefixtures("test_mongo")
def test_create_project(database, command_runner):
    """Test creating a new project."""
    command_runner(["projects", "create", "test_project", "--quota", "2468"])

    project = database.projects.get("test_project")
    assert project["quota"] == 2468
    assert project["used_quota"] == 0


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


@pytest.mark.usefixtures("test_mongo")
def test_modify_project(command_runner):
    """Test setting new quota for a project"""
    db.Database().projects.create("test_project", quota=2048)

    result = command_runner([
        "projects", "modify", "test_project", "--quota", "1"
    ])

    # Assert that quota has actually changed
    project = db.Database().projects.get("test_project")
    assert project["quota"] == 1

    # Assert that output tells the new quota
    data = json.loads(result.output)
    assert data["quota"] == 1


@pytest.mark.usefixtures("test_mongo")
def test_modify_project_fail(command_runner):
    """Test modifying a project that does not exist"""
    result = command_runner(["projects", "modify", "test_project"])

    assert result.output == "Project 'test_project' does not exist.\n"
