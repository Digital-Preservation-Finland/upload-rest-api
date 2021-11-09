"""Tests for ``upload_rest_api.__main__`` module."""
import datetime
import pathlib
import sys

import mock
import pytest
import upload_rest_api.__main__
import upload_rest_api.database as db


@mock.patch('upload_rest_api.__main__.clean_mongo')
@mock.patch('upload_rest_api.__main__.clean_disk')
@pytest.mark.parametrize("command", ("files", "mongo"))
def test_cleanup(mock_clean_disk, mock_clean_mongo, command):
    """Test that correct function is called from main function when
    cleanup-files or cleanup-mongo command is used.
    """
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'cleanup-%s' % command]
    ):
        upload_rest_api.__main__.main()

    if command == "files":
        mock_clean_disk.assert_called()
        mock_clean_mongo.assert_not_called()
    else:
        mock_clean_disk.assert_not_called()
        mock_clean_mongo.assert_called()


def test_cleanup_tokens(database, capsys, monkeypatch):
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

    monkeypatch.setattr("sys.argv", ["upload-rest-api", "cleanup-tokens"])

    upload_rest_api.__main__.main()

    # Only the last token exists
    out, _ = capsys.readouterr()
    assert out == "Cleaned 2 expired token(s)\n"

    assert database.tokens.tokens.count() == 1
    token = next(database.tokens.tokens.find())
    assert token["name"] == "Token 3"


@pytest.mark.usefixtures('test_mongo')
def test_get(capsys):
    """Test get command."""
    database = db.Database()
    database.user("test1").create(projects=["test_project"])
    database.user("test2").create(projects=["test_project"])

    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'get', '--users']
    ):
        upload_rest_api.__main__.main()

    out, _ = capsys.readouterr()
    assert out == "test1\ntest2\n"


def test_create_user(test_mongo, mock_config):
    """Test creating user test.

    User should be added to database and project directory should be
    created.
    """
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'create-user', 'test_user']
    ):
        upload_rest_api.__main__.main()

    assert test_mongo.upload.users.count({"_id": "test_user"}) == 1


@pytest.mark.usefixtures('test_mongo')
def test_create_existing_user():
    """Test that creating a user that already exists raises
    UserExistsError.
    """
    db.Database().user("test").create(projects=["test_project"])
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'create-user', 'test']
    ):
        with pytest.raises(db.UserExistsError):
            upload_rest_api.__main__.main()


def test_delete_user(test_mongo):
    """Test deletion of an existing user."""
    db.Database().user("test").create(projects=["test_project"])
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'delete-user', 'test']
    ):
        upload_rest_api.__main__.main()

    assert test_mongo.upload.users.count({"_id": "test"}) == 0


@pytest.mark.usefixtures('test_mongo')
def test_delete_user_fail():
    """Test deletion of an user that does not exist."""
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'delete-user', 'test']
    ):
        with pytest.raises(db.UserNotFoundError):
            upload_rest_api.__main__.main()


@pytest.mark.usefixtures('test_mongo')
def test_grant_user_projects(database, monkeypatch):
    """Test granting the user access to projects."""
    user = db.Database().user("test")
    user.create(projects=["test_project"])

    database.projects.create("test_project_2", 2000)
    database.projects.create("test_project_3", 2000)

    monkeypatch.setattr(
        sys, "argv",
        [
            'upload-rest-api', 'grant-user-projects', "test",
            "test_project_2", "test_project_3"
        ]
    )

    upload_rest_api.__main__.main()

    assert user.get_projects() == [
        "test_project", "test_project_2", "test_project_3"
    ]


@pytest.mark.usefixtures('test_mongo')
def test_grant_user_projects_nonexistent_project(database, monkeypatch):
    """Test granting the user access to project that does not exist."""
    user = db.Database().user("test")
    user.create(projects=["test_project"])

    monkeypatch.setattr(
        sys, 'argv',
        [
            'upload-rest-api', 'grant-user-projects', "test",
            "test_project_2"
        ]
    )

    with pytest.raises(db.ProjectNotFoundError) as exc:
        upload_rest_api.__main__.main()

    assert str(exc.value) == "Project 'test_project_2' not found"


@pytest.mark.usefixtures('test_mongo')
def test_grant_user_projects_nonexistent_user(database, monkeypatch):
    """Test granting a nonexistent user access to a project."""
    database.projects.create("test_project")

    monkeypatch.setattr(
        sys, 'argv',
        [
            'upload-rest-api', 'grant-user-projects', "fake_user",
            "test_project"
        ]
    )

    with pytest.raises(db.UserNotFoundError) as exc:
        upload_rest_api.__main__.main()

    assert str(exc.value) == "User 'fake_user' not found"


@pytest.mark.usefixtures('test_mongo')
def test_revoke_user_projects(database, monkeypatch):
    """Test granting the user access to projects."""
    user = db.Database().user("test")
    user.create(projects=["test_project"])

    monkeypatch.setattr(
        sys, "argv",
        [
            'upload-rest-api', 'revoke-user-projects', "test",
            "test_project"
        ]
    )

    upload_rest_api.__main__.main()

    assert user.get_projects() == []


@pytest.mark.usefixtures("test_mongo")
def test_create_project(database, monkeypatch):
    """Test creating a new project."""
    monkeypatch.setattr(
        sys, "argv",
        [
            "upload-rest-api", "create-project",
            "test_project", "--quota", "2468"
        ]
    )

    upload_rest_api.__main__.main()

    project = database.projects.get("test_project")
    assert project["quota"] == 2468
    assert project["used_quota"] == 0


@pytest.mark.usefixtures("test_mongo")
def test_create_project_already_exists(database, monkeypatch):
    """Test creating a project that already exists."""
    database.projects.create("test_project", quota=2048)

    monkeypatch.setattr(
        sys, "argv",
        [
            "upload-rest-api", "create-project", "test_project",
            "--quota", "2048"
        ]
    )

    with pytest.raises(db.ProjectExistsError) as exc:
        upload_rest_api.__main__.main()

    assert str(exc.value) == "Project 'test_project' already exists"


@pytest.mark.usefixtures("test_mongo")
def test_delete_project(database, monkeypatch):
    """Test deleting a project"""
    database.projects.create("test_project", quota=2048)

    monkeypatch.setattr(
        sys, "argv",
        ["upload-rest-api", "delete-project", "test_project"]
    )

    upload_rest_api.__main__.main()

    assert not database.projects.get("test_project")
