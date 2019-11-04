"""Unit tests for module authentication"""
from __future__ import unicode_literals

import pytest

import upload_rest_api.authentication as auth
import upload_rest_api.database


@pytest.mark.parametrize(
    ('user', 'password', 'result'),
    [
        ('test_user', 'test_password', True),
        ('test_user', 'false_password', False),
        ('false_user', 'test_password', False)
    ]
)
def test_auth_user(user, password, result):
    """Test _auth_user() function with different username-password combinations.

    :param user: username of user
    :param password: password of user
    :param bool result: Excepted result of authentication
    """
    # Create one test user to database
    usersdoc = upload_rest_api.database.UsersDoc('test_user')
    usersdoc.create('test_project', 'test_password')

    # pylint: disable=protected-access
    assert auth._auth_user(user, password) is result
