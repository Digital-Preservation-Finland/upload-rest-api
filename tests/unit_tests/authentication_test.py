"""Unit tests for module authentication."""
import pytest

import upload_rest_api.authentication as auth
import upload_rest_api.database as db


@pytest.mark.parametrize(
    ('user', 'password', 'result'),
    [
        ('test_user', 'test_password', True),
        ('test_user', 'false_password', False),
        ('false_user', 'test_password', False)
    ]
)
def test_auth_user_by_password(test_client, user, password, result):
    """Test HTTP Basic authentication using different username-password
    combinations.

    :param user: username of user
    :param password: password of user
    :param bool result: Excepted result of authentication
    """
    # Create one test user to database
    usersdoc = db.Database().user('test_user')
    usersdoc.create('test_project', 'test_password')

    # pylint: disable=protected-access
    response = test_client.get("/v1/", auth=(user, password))
    if result:
        # Authentication passes, and 404 is returned
        assert response.status_code == 404
    else:
        # Authentication shouldn't pass, and 401 should be returned
        assert response.status_code == 401
