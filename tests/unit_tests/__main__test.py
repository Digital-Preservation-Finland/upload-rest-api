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
    database.user("test1").create("test_project")
    database.user("test2").create("test_project")

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
        ['upload-rest-api', 'create', 'test_user', 'test_project']
    ):
        upload_rest_api.__main__.main()

    assert test_mongo.upload.users.count({"_id": "test_user"}) == 1
    assert pathlib.Path(mock_config['UPLOAD_PATH'], 'test_project').exists()


@pytest.mark.usefixtures('test_mongo')
def test_create_existing_user():
    """Test that creating a user that already exists raises
    UserExistsError.
    """
    db.Database().user("test").create("test_project")
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'create', 'test', 'test']
    ):
        with pytest.raises(db.UserExistsError):
            upload_rest_api.__main__.main()


def test_delete_user(test_mongo):
    """Test deletion of an existing user."""
    db.Database().user("test").create("test_project")
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'delete', 'test']
    ):
        upload_rest_api.__main__.main()

    assert test_mongo.upload.users.count({"_id": "test"}) == 0


@pytest.mark.usefixtures('test_mongo')
def test_delete_user_fail():
    """Test deletion of an user that does not exist."""
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'delete', 'test']
    ):
        with pytest.raises(db.UserNotFoundError):
            upload_rest_api.__main__.main()


@pytest.mark.usefixtures('test_mongo')
def test_modify():
    """Test modifying user quota and project."""
    user = db.Database().user("test")
    user.create("test_project")
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'modify', 'test', "--quota", "1", "--project", "X"]
    ):
        upload_rest_api.__main__.main()

    assert user.get_quota() == 1
    assert user.get_project() == "X"
