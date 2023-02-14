"""Unit tests for user API."""
from base64 import b64encode

from upload_rest_api.models.token import Token
from upload_rest_api.models.user import User


def test_get_user_projects_by_token(test_client):
    """Test retrieving a list of projects using a token.

    The list should contain projects that the user can access according
    to the token.
    """
    # Create a user
    User.create(username="test_user",
                projects=["test_project2"],
                password="test_password")

    # Create a token
    token_data = Token.create(
        name="User test token",
        username="test_user",
        projects=["test_project"],
        expiration_date=None,
        admin=False
    )

    auth_headers = {"Authorization": f"Bearer {token_data['token']}"}
    response = test_client.get(
        "/v1/users/projects",
        headers=auth_headers
    )

    # User token provides access to projects defined by token, but not
    # to projects defined in user database.
    assert response.status_code == 200
    assert response.json == {
        "projects": [
            {
                "identifier": "test_project",
                "used_quota": 0,
                "quota": 1000000
            }
        ]
    }


def test_get_user_projects_by_password(test_client):
    """Test retrieving a list of projects using HTTP Basic Auth.

    The list should contain projects that the user can access according
    to the database.
    """
    # Create a user
    User.create(username="test_user",
                projects=["test_project", "test_project2"],
                password="test_password")

    auth_headers = {
        "Authorization": "Basic %s"
        % b64encode(b"test_user:test_password").decode("utf-8")
    }
    response = test_client.get("/v1/users/projects", headers=auth_headers)

    assert response.status_code == 200
    assert response.json == {
        "projects": [
            {
                "identifier": "test_project",
                "used_quota": 0,
                "quota": 1000000
            },
            {
                "identifier": "test_project2",
                "used_quota": 0,
                "quota": 12345678
            }
        ]
    }


def test_get_user_projects_by_username_admin(test_client, admin_auth):
    """
    Test retrieving a list of projects for a specific user when
    authenticated as an admin
    """
    # Create a user
    User.create(username="test_user",
                projects=["test_project"],
                password="test_password")

    response = test_client.get(
        "/v1/users/projects",
        query_string={"username": "test_user"},
        headers=admin_auth
    )

    assert response.status_code == 200
    assert response.json == {
        "projects": [
            {
                "identifier": "test_project",
                "used_quota": 0,
                "quota": 1000000
            }
        ]
    }


def test_get_user_projects_by_username_user(test_client, test_auth):
    """Try retrieving list of projects for user defined by query string.

    Only admins should be able list projects of specific users, so API
    should respond with HTTP error "403 Forbidden".
    """
    # Create a user
    User.create(username="test_user",
                projects=["test_project2"],
                password="test_password")

    # User can not retrieve even their own projects
    response = test_client.get(
        "/v1/users/projects",
        query_string={"username": "test_user"},
        headers=test_auth
    )
    assert response.status_code == 403
    assert response.json == {
        'code': 403,
        'error': 'User does not have permission to list projects'
    }
