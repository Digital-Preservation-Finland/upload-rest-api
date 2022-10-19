"""Unit tests for module authentication."""
import base64
import datetime

import pytest
from upload_rest_api.models import User, Token


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
    User.create(
        "test_user", projects=["test_project"], password="test_password"
    )

    auth_hash \
        = base64.urlsafe_b64encode(f"{user}:{password}".encode()).decode()
    auth_header = f"Basic {auth_hash}"

    # pylint: disable=protected-access
    response = test_client.get(
        "/v1/", headers={"Authorization": auth_header}
    )
    if result:
        # Authentication passes, and 404 is returned
        assert response.status_code == 404
    else:
        # Authentication shouldn't pass, and 401 should be returned
        assert response.status_code == 401


@pytest.mark.parametrize(
    "options,is_valid",
    [
        (
            # Expiration date 5 minutes in the future - valid
            {
                "expiration_date": (
                    datetime.datetime.now(datetime.timezone.utc)
                    + datetime.timedelta(minutes=5)
                ),
            },
            True
        ),
        (
            # Expiration date 5 minutes in the past - invalid
            {
                "expiration_date": (
                    datetime.datetime.now(datetime.timezone.utc)
                    + datetime.timedelta(minutes=-5)
                ),
            },
            False
        ),
        (
            # No expiration date - valid
            {}, True
        ),
        (
            # No access to the correct project - invalid
            {
                "projects": ["test_project_2", "test_project_3"]
            },
            False
        ),
        (
            # Admin has access to every project - valid
            {
                "projects": [],
                "admin": True
            },
            True
        )
    ]
)
def test_auth_user_by_token(test_client, options, is_valid):
    """
    Create a token and test authenticating using it
    """
    kwargs = {
        "name": "User test token #1",
        "username": "test",
        "projects": ["test_project", "project"],
        "expiration_date": None,
        "admin": False
    }
    kwargs.update(options)
    token_data = Token.create(**kwargs)
    token = token_data["token"]

    response = test_client.get(
        "/v1/files/test_project/fake_file.txt",
        headers={"Authorization": f"Bearer {token}"}
    )

    if is_valid:
        # Authentication passes, file not found
        assert response.status_code == 404
    else:
        # Authentication does not pass due to expired token
        assert response.status_code in (401, 403)
