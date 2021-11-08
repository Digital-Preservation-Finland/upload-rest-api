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
