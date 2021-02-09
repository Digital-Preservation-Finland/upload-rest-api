"""Tests for ``upload_rest_api.__main__`` module."""
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


@pytest.mark.usefixtures('mock_mongo')
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


def test_create_user(mock_mongo):
    """Test creating user test."""
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'create', 'test', 'test']
    ):
        upload_rest_api.__main__.main()

    assert mock_mongo.upload.users.count({"_id": "test"}) == 1


@pytest.mark.usefixtures('mock_mongo')
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


def test_delete_user(mock_mongo):
    """Test deletion of an existing user."""
    db.Database().user("test").create("test_project")
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'delete', 'test']
    ):
        upload_rest_api.__main__.main()

    assert mock_mongo.upload.users.count({"_id": "test"}) == 0


@pytest.mark.usefixtures('mock_mongo')
def test_delete_user_fail():
    """Test deletion of an user that does not exist."""
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'delete', 'test']
    ):
        with pytest.raises(db.UserNotFoundError):
            upload_rest_api.__main__.main()


# @pytest.mark.usefixtures('mock_mongo')
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
