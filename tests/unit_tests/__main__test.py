"""Tests for ``upload_rest_api.__main__`` module"""
import sys

import mock
import pytest

import upload_rest_api.__main__
import upload_rest_api.database as db


@mock.patch('upload_rest_api.__main__.clean_mongo')
@mock.patch('upload_rest_api.__main__.clean_disk')
@pytest.mark.parametrize("command", ("disk", "mongo"))
def test_cleanup(mock_clean_disk, mock_clean_mongo, command):
    """Test that correct function is called from main function when "cleanup"
    command is used.
    """
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'cleanup', command]
    ):
        upload_rest_api.__main__.main()

    if command == "disk":
        mock_clean_disk.assert_called()
        mock_clean_mongo.assert_not_called()
    else:
        mock_clean_disk.assert_not_called()
        mock_clean_mongo.assert_called()


def test_create_user(mock_mongo):
    """Test creating user test"""
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'create', 'test', 'test']
    ):
        upload_rest_api.__main__.main()

    assert mock_mongo.upload.users.count({"_id": "test"}) == 1


def test_create_existing_user(mock_mongo):
    """Test that creating a user that already exists raises UserExistsError"""
    db.UsersDoc("test").create("test_project")
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'create', 'test', 'test']
    ):
        with pytest.raises(db.UserExistsError):
            upload_rest_api.__main__.main()


def test_delete_user(mock_mongo):
    """Test deletion of an existing user"""
    db.UsersDoc("test").create("test_project")
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'delete', 'test']
    ):
        upload_rest_api.__main__.main()

    assert mock_mongo.upload.users.count({"_id": "test"}) == 0


def test_delete_user_fail(mock_mongo):
    """Test deletion of an user that does not exist"""
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'delete', 'test']
    ):
        with pytest.raises(db.UserNotFoundError):
            upload_rest_api.__main__.main()


def test_modify(mock_mongo):
    """Test modifying user quota and project"""
    user = db.UsersDoc("test")
    user.create("test_project")
    with mock.patch.object(
        sys, 'argv',
        ['upload-rest-api', 'modify', 'test', "--quota", "1", "--project", "X"]
    ):
        upload_rest_api.__main__.main()

    assert user.get_quota() == 1
    assert user.get_project() == "X"
