"""Unit tests for user API"""


def test_get_user_projects_by_token(test_client, user_token_auth):
    """
    Test retrieving a list of projects when authenticated using a token
    """
    response = test_client.get(
        "/v1/users/projects",
        headers=user_token_auth
    )

    # User token provides access to both 'project' and 'test_project'
    assert response.status_code == 200
    assert response.json == {
        "projects": [
            {
                "identifier": "test_project",
                "used_quota": 0,
                "quota": 1000000
            },
            {
                "identifier": "project",
                "used_quota": 0,
                "quota": 12345678
            },
        ]
    }


def test_get_user_projects_by_password(test_client, test_auth):
    """
    Test retrieving a list of projects when authenticated using HTTP Basic Auth
    """
    response = test_client.get(
        "/v1/users/projects",
        headers=test_auth
    )

    # 'test' user provides access to only 'test_project'
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


def test_get_user_projects_by_username_admin(test_client, admin_auth):
    """
    Test retrieving a list of projects for a specific user when
    authenticated as an admin
    """
    response = test_client.get(
        "/v1/users/projects",
        query_string={"username": "test"},
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
    """Try retrieving list of projects for a different user and ensure
    that the user is unable to do so.
    """
    # User can retrieve their own projects
    response = test_client.get(
        "/v1/users/projects",
        query_string={"username": "test"},
        headers=test_auth
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

    # User can't retrieve other users' projects
    response = test_client.get(
        "/v1/users/projects",
        query_string={"username": "test_2"},
        headers=test_auth
    )

    assert response.status_code == 403
    assert response.json == {
        "code": 403,
        "error": "User does not have permission to list projects"
    }
