"""Tests for `upload_rest_api.api.v1.tokens` module"""

import pytest

from upload_rest_api.models import Token, TokenEntry, User


def test_create_token(test_client, admin_auth):
    """
    Create a token using the `/create` API endpoint
    """
    response = test_client.post(
        "/v1/tokens/create",
        data={
            "name": "Test token",
            "username": "sso_test_user",
            "projects": ",".join(["test_project", "project"]),
        },
        headers=admin_auth
    )

    # Token was created
    token = response.json["token"]
    assert token

    # Token has correct permissions
    token_data = Token.get_by_token(token)

    assert token_data.name == "Test token"
    assert token_data.username == "sso_test_user"
    assert token_data.projects == ["test_project", "project"]
    assert not token_data.expiration_date
    assert not token_data.admin
    assert not token_data.session


def test_create_token_permission_denied(test_client, user_token_auth):
    """
    Try creating a token using an user token which doesn't have the necessary
    permissions
    """
    response = test_client.post(
        "/v1/tokens/create",
        data={
            "name": "Test token",
            "username": "sso_test_user",
            "projects": ""
        },
        headers=user_token_auth
    )

    # User tokens are not allowed to create new tokens
    assert response.status_code == 403
    assert response.json["error"] == \
        "User does not have permission to create tokens"


@pytest.mark.parametrize(
    "params,expected_error",
    [
        (
            {"username": "test", "projects": ""},
            "'name' is required"
        ),
        (
            {"name": "Test token", "username": "test"},
            "'projects' is required"
        ),
        (
            {"name": "Test token", "projects": "test1,test2"},
            "'username' is required"
        ),
        (
            {"name": "x"*1025, "username": "test", "projects": "test"},
            "'name' maximum length is 1024 characters"
        ),
        (
            {
                "name": "Test token",
                "username": "test",
                "projects": "",
                "expiration_date": "2021-07-26T12:06:02.559885+00:00"
            },
            "'expiration_date' has already expired"
        )
    ]
)
def test_create_token_error(test_client, admin_auth, params, expected_error):
    """
    Try creating tokens with different validation errors
    """
    response = test_client.post(
        "/v1/tokens/create",
        data=params,
        headers=admin_auth
    )

    assert response.status_code == 400
    assert response.json["error"] == expected_error


def test_create_session_token(test_client, admin_auth):
    """
    Create a token using the `/create_session` API endpoint
    """
    response = test_client.post(
        "/v1/tokens/create_session",
        data={"username": "test"},
        headers=admin_auth
    )

    # Token was created
    token = response.json["token"]
    assert token

    # Token has correct permissions
    token_data = Token.get_by_token(token)

    assert token_data.name == "test session token"
    assert token_data.username == "test"
    assert token_data.projects == ["test_project"]
    assert token_data.expiration_date
    assert token_data.session


def test_create_session_token_missing_username(test_client, admin_auth):
    """
    Try creating session token with missing username
    """
    response = test_client.post(
        "/v1/tokens/create_session",
        data={},
        headers=admin_auth
    )

    assert response.status_code == 400
    assert response.json["error"] == "'username' is required"


def test_create_session_token_new_user_created(test_client, admin_auth):
    """
    Create a session token for a nonexistent user and ensure that the
    user is automatically created.
    """
    response = test_client.post(
        "/v1/tokens/create_session",
        data={"username": "acme_org/user"},
        headers=admin_auth
    )

    assert response.json["token"]

    # User should be created without any default projects
    user = User.get(username="acme_org/user")
    assert user.username == "acme_org/user"
    assert user.projects == ()


def test_list_tokens(test_client, admin_auth, test_mongo):
    """
    Create multiple tokens and ensure they're included in the token
    listing
    """
    for i in range(0, 5):
        test_client.post(
            "/v1/tokens/create",
            data={
                "name": f"Test token {i}",
                "username": "sso_test_user",
                "projects": ",".join(["test_project", "project"]),
            },
            headers=admin_auth
        )

    response = test_client.get(
        "/v1/tokens/list",
        query_string={
            "username": "sso_test_user"
        },
        headers=admin_auth
    )

    data = response.json
    assert len(data["tokens"]) == 5
    assert data["tokens"][0]["identifier"]
    assert data["tokens"][0]["name"] == "Test token 0"
    assert data["tokens"][0]["username"] == "sso_test_user"
    assert data["tokens"][0]["projects"] == [
        "test_project", "project"
    ]
    assert not data["tokens"][0]["expiration_date"]

    assert data["tokens"][4]["name"] == "Test token 4"


def test_list_tokens_permission_denied(test_client, user_token_auth):
    """
    Try listing tokens using an user token, which is not allowed
    """
    response = test_client.get(
        "/v1/tokens/list",
        query_string={
            "username": "sso_test_user"
        },
        headers=user_token_auth
    )

    assert response.status_code == 403
    assert response.json["error"] == \
        "User does not have permission to list tokens"


def test_delete_token(test_client, admin_auth):
    """
    Create a token and then delete it
    """
    response = test_client.post(
        "/v1/tokens/create",
        data={
            "name": "Test token",
            "username": "sso_test_user",
            "projects": ",".join(["test_project", "project"]),
        },
        headers=admin_auth
    )

    identifier = response.json["identifier"]
    token = response.json["token"]

    response = test_client.delete(
        "/v1/tokens/",
        data={
            "username": "sso_test_user",
            "token_id": identifier
        },
        headers=admin_auth
    )

    assert response.json["deleted"]

    # Token was really deleted
    with pytest.raises(Token.DoesNotExist):
        Token.get_by_token(token)


def test_delete_token_permission_denied(test_client, user_token_auth):
    """
    Try deleting a token using an user token
    """
    token_id = TokenEntry.objects.get(username="test").id
    response = test_client.delete(
        "/v1/tokens/",
        data={
            "username": "test_user",
            "token_id": token_id
        },
        headers=user_token_auth
    )

    assert response.status_code == 403
    assert response.json["error"] == \
        "User does not have permission to delete tokens"


@pytest.mark.usefixtures("user_token_auth")
def test_delete_token_username_not_provided(test_client, admin_auth):
    """
    Try deleting a token without providing an username
    """
    token_id = TokenEntry.objects.get(username="test").id
    response = test_client.delete(
        "/v1/tokens/",
        data={
            "token_id": token_id
        },
        headers=admin_auth
    )

    assert response.json["error"] == "'username' not provided"


def test_delete_token_token_id_not_provided(test_client, admin_auth):
    """
    Try deleting a token without providing a token ID
    """
    response = test_client.delete(
        "/v1/tokens/",
        data={
            "username": "test_user"
        },
        headers=admin_auth
    )

    assert response.json["error"] == "'token_id' not provided"
