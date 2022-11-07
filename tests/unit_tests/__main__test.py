"""Tests for ``upload_rest_api.__main__`` module."""
import datetime
import json
from unittest import mock

import pytest
from click.testing import CliRunner

import upload_rest_api.__main__
from upload_rest_api.models import (File, Project, ProjectExistsError, Token,
                                    User, UserExistsError)


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


@pytest.mark.parametrize("command", ("files", "mongo", "tus-uploads"))
def test_cleanup(mocker, command, command_runner):
    """Test that correct function is called from main function when
    cleanup command is used.

    :param mocker: pytest-mock mocker
    :param command: command to be run
    :param command_runner: command runner
    """
    mock_clean_mongo = mocker.patch('upload_rest_api.__main__.clean_mongo')
    mock_clean_disk = mocker.patch('upload_rest_api.__main__.clean_disk')
    mock_clean_tus_uploads \
        = mocker.patch('upload_rest_api.__main__.clean_tus_uploads')

    command_runner(["cleanup", command])

    func_to_call = None

    if command == "files":
        func_to_call = mock_clean_disk
    elif command == "mongo":
        func_to_call = mock_clean_mongo
    elif command == "tus-uploads":
        func_to_call = mock_clean_tus_uploads

    all_cli_funcs = (
        mock_clean_disk, mock_clean_mongo, mock_clean_tus_uploads
    )

    for cli_func in all_cli_funcs:
        if cli_func == func_to_call:
            cli_func.assert_called()
        else:
            cli_func.assert_not_called()


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
        Token.create(
            name=name,
            username="test",
            projects=[],
            expiration_date=expiration_date,
            session=True
        )

    # Only the last token exists
    result = command_runner(["cleanup", "tokens"])
    assert result.output == "Cleaned 2 expired token(s)\n"

    assert Token.objects.count() == 1
    token = Token.objects.first()
    assert token["name"] == "Token 3"


@pytest.mark.usefixtures('test_mongo')
def test_list_users(command_runner):
    """Test listing all users."""
    User.create(username="test1", projects=["test_project"])
    User.create(username="test2", projects=["test_project"])

    result = command_runner(["users", "list"])
    assert result.output == "test1\ntest2\n"


@pytest.mark.usefixtures('test_mongo')
def test_list_users_when_no_users(command_runner, database):
    """Test listing all users when there are no users."""
    result = command_runner(["users", "list"])

    assert result.output == "No users found\n"


def test_get_user(command_runner):
    """Test displaying information of one user."""
    User.create(username="test1", projects=["test_project"])

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
    Project.create(identifier="test_project_o", quota=0)
    Project.create(identifier="test_project_q", quota=0)
    Project.create(identifier="test_project_r", quota=0)

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
    Project.create(identifier="test_project_a", quota=1248)

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
    User.create(username="test", projects=["test_project"])
    with pytest.raises(UserExistsError):
        command_runner(["users", "create", "test"])


def test_delete_user(test_mongo, command_runner):
    """Test deletion of an existing user."""
    User.create(username="test", projects=["test_project"])
    command_runner(["users", "delete", "test"])

    assert test_mongo.upload.users.count({"_id": "test"}) == 0


@pytest.mark.usefixtures('test_mongo')
def test_delete_user_fail(command_runner):
    """Test deletion of an user that does not exist."""
    with pytest.raises(User.DoesNotExist):
        command_runner(["users", "delete", "test"])


@pytest.mark.usefixtures('test_mongo')
def test_modify_user(command_runner):
    """Test generating a new password for a user."""
    old_password = User.create("test")

    user = User.objects.get(username="test")
    old_salt = user["salt"]
    old_digest = user["digest"]

    response = command_runner([
        "users", "modify", "test", "--generate-password"
    ])

    # Assert that password has actually changed
    user = User.objects.get(username="test")
    assert user.salt != old_salt
    assert user.digest != old_digest

    # Assert that output contains new password
    data = json.loads(response.output)
    assert data["password"]
    assert data["password"] != old_password


@pytest.mark.usefixtures('test_mongo')
def test_modify_user_fail(command_runner):
    """Test modifying a user that does not exist."""
    with pytest.raises(User.DoesNotExist):
        command_runner(["users", "modify", "test"])


@pytest.mark.usefixtures('test_mongo')
def test_grant_user_projects(database, command_runner):
    """Test granting the user access to projects."""
    User.create(username="test", projects=["test_project"])

    Project.create("test_project_2", 2000)
    Project.create("test_project_3", 2000)

    result = command_runner([
        "users", "project-rights", "grant", "test", "test_project_2",
        "test_project_3"
    ])

    user = User.objects.get(username="test")

    assert user.projects == [
        "test_project", "test_project_2", "test_project_3"
    ]
    assert result.output == (
        "Granted user 'test' access to project(s): "
        "test_project_2, test_project_3\n"
    )


@pytest.mark.usefixtures('test_mongo')
def test_grant_user_projects_nonexistent_project(database, command_runner):
    """Test granting the user access to project that does not exist."""
    User.create("test", projects=["test_project"])

    with pytest.raises(Project.DoesNotExist) as exc:
        command_runner([
            "users", "project-rights", "grant", "test", "test_project_2"
        ])

    assert str(exc.value) == "Project matching query does not exist."


@pytest.mark.usefixtures('test_mongo')
def test_grant_user_projects_nonexistent_user(
        database, monkeypatch, command_runner):
    """Test granting a nonexistent user access to a project."""
    Project.create(identifier="test_project", quota=0)

    with pytest.raises(User.DoesNotExist) as exc:
        command_runner([
            "users", "project-rights", "grant", "fake_user", "test_project"
        ])

    assert str(exc.value) == "User matching query does not exist."


@pytest.mark.usefixtures('test_mongo')
def test_revoke_user_projects(database, command_runner):
    """Test revoking the user access to projects."""
    User.create(username="test", projects=["test_project"])

    result = command_runner([
        "users", "project-rights", "revoke", "test", "test_project"
    ])

    user = User.objects.get(username="test")

    assert user.projects == []
    assert result.output == (
        "Revoked user 'test' access to project(s): test_project\n"
    )


@pytest.mark.parametrize("quota", [0, 2468])
@pytest.mark.usefixtures("test_mongo")
def test_create_project(database, command_runner, quota):
    """Test creating a new project."""
    command_runner(["projects", "create", "test_project", "--quota", quota])

    project = Project.objects.get(id="test_project")
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
    # Newer versions of click use single quotes, while older versions use
    # double quotes around '--quota', causing problems in the pipeline.
    # As a workaround check output in pieces
    assert "Invalid value for" in result.output
    assert "--quota" in result.output


@pytest.mark.usefixtures("test_mongo")
def test_create_project_already_exists(database, command_runner):
    """Test creating a project that already exists."""
    Project.create(identifier="test_project", quota=2048)

    with pytest.raises(ProjectExistsError) as exc:
        command_runner([
            "projects", "create", "test_project", "--quota", "2048"
        ])

    assert str(exc.value) == "Project 'test_project' already exists"


@pytest.mark.usefixtures("test_mongo")
def test_delete_project(database, command_runner):
    """Test deleting a project"""
    Project.create(identifier="test_project", quota=2048)

    command_runner(["projects", "delete", "test_project"])

    with pytest.raises(Project.DoesNotExist):
        Project.objects.get(id="test_project")


@pytest.mark.parametrize("quota", [0, 1])
@pytest.mark.usefixtures("test_mongo")
def test_modify_project(command_runner, quota):
    """Test setting new quota for a project"""
    Project.create(identifier="test_project", quota=2048)

    result = command_runner([
        "projects", "modify", "test_project", "--quota", quota
    ])

    # Assert that quota has actually changed
    project = Project.objects.get(id="test_project")
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
    # Newer versions of click use single quotes, while older versions use
    # double quotes around '--quota', causing problems in the pipeline.
    # As a workaround check output in pieces
    assert "Invalid value for" in result.output
    assert "--quota" in result.output


@pytest.mark.usefixtures("test_mongo")
def test_modify_project_fail(command_runner):
    """Test modifying a project that does not exist"""
    result = command_runner(["projects", "modify", "test_project"])

    assert result.output == "Project 'test_project' does not exist.\n"


def test_get_file_by_path(command_runner, database):
    """Test displaying information of file specified by path."""
    File(
        path="/path_1", checksum="checksum_1", identifier="pid:urn:1"
    ).save()

    result = command_runner(["files", "get", "path", "/path_1"])
    result_data = json.loads(result.output)
    correct_result = {
        "_id": "/path_1",
        "checksum": "checksum_1",
        "identifier": "pid:urn:1"
    }
    assert result_data == correct_result


def test_get_file_by_identifier(command_runner, database):
    """Test displaying information of file specified by identifier."""
    File(
        path="/path_1", checksum="checksum_1", identifier="pid:urn:1"
    ).save()

    result = command_runner(["files", "get", "identifier", "pid:urn:1"])
    result_data = json.loads(result.output)
    correct_result = {
        "_id": "/path_1",
        "checksum": "checksum_1",
        "identifier": "pid:urn:1"
    }
    assert result_data == correct_result


def test_list_files(database, command_runner):
    """Test listing all files."""
    File.objects.insert([
        File(path="/path_1", identifier="pid:urn:1", checksum="checksum_1"),
        File(path="/path_2", identifier="pid:urn:2", checksum="checksum_2")
    ])

    result = command_runner(["files", "list"])
    result_data = json.loads(result.output)
    assert result_data == [
        {"_id": "/path_1",
         "identifier": "pid:urn:1",
         "checksum": "checksum_1"},
        {"_id": "/path_2",
         "identifier": "pid:urn:2",
         "checksum": "checksum_2"}
    ]


def test_list_file_identifiers(database, command_runner):
    """Test listing all file identifiers."""
    File.objects.insert([
        File(path="/path_1", identifier="pid:urn:1", checksum="checksum_1"),
        File(path="/path_2", identifier="pid:urn:2", checksum="checksum_2")
    ])

    result = command_runner(["files", "list", "--identifiers-only"])
    result_data = json.loads(result.output)
    assert result_data == ["pid:urn:1", "pid:urn:2"]


def test_list_checksums(database, command_runner):
    """Test listing all file checksums."""
    File.objects.insert([
        File(path="/path_1", identifier="pid:urn:1", checksum="checksum_1"),
        File(path="/path_2", identifier="pid:urn:2", checksum="checksum_2")
    ])

    result = command_runner(["files", "list", "--checksums-only"])
    result_data = json.loads(result.output)
    assert result_data == ["checksum_1", "checksum_2"]


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
